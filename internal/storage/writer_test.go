package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// ---- fakeFile ---------------------------------------------------------------

// fakeFile implements fileWriter. Write returns ENOSPC for the first failFirst
// calls, then succeeds. Sync calls are counted atomically.
type fakeFile struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	syncCount atomic.Int64
	failFirst int
	failsDone int
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
func (f *fakeFile) Sync() error  { f.syncCount.Add(1); return nil }
func (f *fakeFile) Close() error { f.mu.Lock(); defer f.mu.Unlock(); f.closed = true; return nil }
func (f *fakeFile) Fd() uintptr  { return 0 }

func (f *fakeFile) FailsDone() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failsDone
}
func (f *fakeFile) Lines() []string {
	f.mu.Lock()
	data := make([]byte, f.buf.Len())
	copy(data, f.buf.Bytes())
	f.mu.Unlock()
	var lines []string
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	return lines
}

// ---- fakeSegmentFactory -----------------------------------------------------

// fakeSegmentFactory always returns the same fakeFile regardless of path.
// This means all segments share one buffer, which is fine for tests that
// don't exercise rolling.
type fakeSegmentFactory struct{ f *fakeFile }

func (sf *fakeSegmentFactory) openSegment(_ string) (fileWriter, error)   { return sf.f, nil }
func (sf *fakeSegmentFactory) createSegment(_ string) (fileWriter, error) { return sf.f, nil }

// ---- helpers ----------------------------------------------------------------

func makeTestEnvelope(t *testing.T, orderID string) *envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope("orders", "host", "com.test.Order",
		map[string]string{"order_id": orderID})
	require.NoError(t, err)
	return &env
}

// newFakeWriter creates a channelWriter backed by a single fakeFile.
// maxSegmentBytes=0 disables rolling (all writes go to the same fake file).
// syncIntervalMS: 0 syncs after every write, > 0 syncs periodically.
func newFakeWriter(syncIntervalMS int, notifyFn func()) (*channelWriter, *fakeFile, *testutil.FakeLogger) {
	f := &fakeFile{}
	log := &testutil.FakeLogger{}
	sf := &fakeSegmentFactory{f: f}
	w := newChannelWriterWithFactory("", 0, sf, syncIntervalMS, notifyFn, log)
	return w, f, log
}

// readAllSegments opens every segment in channelDir, reads all JSONL lines,
// and returns them in order.
func readAllSegments(t *testing.T, channelDir string) []string {
	t.Helper()
	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	var lines []string
	for _, seg := range segs {
		f, err := os.Open(seg.path)
		require.NoError(t, err)
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		require.NoError(t, sc.Err())
		_ = f.Close()
	}
	return lines
}

// ---- tests ------------------------------------------------------------------

func TestWriter_Sequential(t *testing.T) {
	w, f, _ := newFakeWriter(1000000, nil)
	const n = 1000
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
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
	w, f, _ := newFakeWriter(1000000, nil)
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
		}()
	}
	wg.Wait()
	require.NoError(t, w.Close())

	lines := f.Lines()
	require.Len(t, lines, goroutines)
	for _, line := range lines {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &v))
	}
}

func TestWriter_SyncAlwaysWhenZero(t *testing.T) {
	w, f, _ := newFakeWriter(0, nil)
	const n = 10
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
	}
	require.NoError(t, w.Close())
	assert.Equal(t, int64(n), f.syncCount.Load(), "syncIntervalMS=0 must Sync once per write")
}

func TestWriter_SyncPeriodic(t *testing.T) {
	const intervalMS = 50
	w, f, _ := newFakeWriter(intervalMS, nil)
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
	assert.Zero(t, f.syncCount.Load(), "syncIntervalMS>0 must not Sync on each write")
	time.Sleep(3 * time.Duration(intervalMS) * time.Millisecond)
	assert.GreaterOrEqual(t, f.syncCount.Load(), int64(1), "syncIntervalMS>0 must Sync on tick")
	require.NoError(t, w.Close())
}

func TestWriter_PeriodicNoSyncWithoutInterval(t *testing.T) {
	w, f, _ := newFakeWriter(1000000, nil)
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Close())
	assert.Zero(t, f.syncCount.Load(), "periodic sync without waiting for interval must not Sync")
}

func TestWriter_Backpressure(t *testing.T) {
	const failFirst = 5
	ff := &fakeFile{failFirst: failFirst}
	sf := &fakeSegmentFactory{f: ff}
	w := newChannelWriterWithFactory("", 0, sf, 1000000, nil, nil)
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord-1")))
	require.NoError(t, w.Close())

	lines := ff.Lines()
	require.Len(t, lines, 1, "record must appear after retries")
	var v map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &v))
	assert.Equal(t, failFirst, ff.FailsDone())
}

func TestWriter_NotifyFn(t *testing.T) {
	var calls atomic.Int64
	w, _, _ := newFakeWriter(1000000, func() { calls.Add(1) })
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Close())
	assert.Equal(t, int64(2), calls.Load(), "notifyFn must be called once per write")
}

func TestWriter_Close_SubsequentWriteErrors(t *testing.T) {
	w, _, _ := newFakeWriter(1000000, nil)
	require.NoError(t, w.Close())
	require.ErrorIs(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")), ErrWriterClosed)
}

func TestWriter_Close_Idempotent(t *testing.T) {
	w, _, _ := newFakeWriter(1000000, nil)
	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close())
}

func TestWriter_RealFile(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")
	w, err := NewChannelWriter(channelDir, 0, 1000000, nil, nil)
	require.NoError(t, err)

	const n = 100
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
	}
	require.NoError(t, w.Close())

	lines := readAllSegments(t, channelDir)
	assert.Len(t, lines, n)
	for i, line := range lines {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &v), "line %d invalid JSON", i)
	}
}

func TestWriter_RealFile_Concurrent(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")
	w, err := NewChannelWriter(channelDir, 0, 1000000, nil, nil)
	require.NoError(t, err)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
		}()
	}
	wg.Wait()
	require.NoError(t, w.Close())

	lines := readAllSegments(t, channelDir)
	assert.Len(t, lines, goroutines)
	for _, line := range lines {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &v))
	}
}

// TestWriter_SegmentRolling verifies that the writer creates a new segment file
// when the current one would exceed maxSegmentBytes.
func TestWriter_SegmentRolling(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")

	// Small segment limit so a few writes trigger a roll.
	const maxSeg = 200
	w, err := NewChannelWriter(channelDir, maxSeg, 1000000, nil, nil)
	require.NoError(t, err)

	const n = 20
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")))
	}
	require.NoError(t, w.Close())

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	assert.Greater(t, len(segs), 1, "multiple segments must be created")

	// All n lines must be recoverable across all segments.
	lines := readAllSegments(t, channelDir)
	assert.Len(t, lines, n)

	// Segment start offsets must be monotonically increasing and consistent
	// with file sizes.
	for i := 1; i < len(segs); i++ {
		expected := segs[i-1].startOffset + segs[i-1].size
		assert.Equal(t, expected, segs[i].startOffset,
			"segment %d startOffset must equal previous segment end", i)
	}
}

// makeTestBatch builds n envelopes with distinct, ordered order_id payloads.
func makeTestBatch(t *testing.T, n int) []*envelope.Envelope {
	t.Helper()
	envs := make([]*envelope.Envelope, n)
	for i := 0; i < n; i++ {
		envs[i] = makeTestEnvelope(t, fmt.Sprintf("ord-%d", i))
	}
	return envs
}

// orderIDOf extracts payload.order_id from a marshalled envelope line.
func orderIDOf(t *testing.T, line string) string {
	t.Helper()
	var v struct {
		Payload struct {
			OrderID string `json:"order_id"`
		} `json:"payload"`
	}
	require.NoError(t, json.Unmarshal([]byte(line), &v))
	return v.Payload.OrderID
}

// TestWriter_WriteBatch_OrderedAndComplete verifies a batch writes every record
// once, in the supplied order.
func TestWriter_WriteBatch_OrderedAndComplete(t *testing.T) {
	w, f, _ := newFakeWriter(1000000, nil)
	const n = 50
	require.NoError(t, w.WriteBatch(context.Background(), makeTestBatch(t, n)))
	require.NoError(t, w.Close())

	lines := f.Lines()
	require.Len(t, lines, n)
	for i, line := range lines {
		assert.Equal(t, fmt.Sprintf("ord-%d", i), orderIDOf(t, line), "record %d out of order", i)
	}
}

// TestWriter_WriteBatch_SingleSyncAndNotify is the core amortisation guarantee:
// with syncIntervalMS=0 (strict durability) a batch of n records performs
// exactly one fsync and one notify, not n of each.
func TestWriter_WriteBatch_SingleSyncAndNotify(t *testing.T) {
	var notifies atomic.Int64
	w, f, _ := newFakeWriter(0, func() { notifies.Add(1) })
	const n = 25
	require.NoError(t, w.WriteBatch(context.Background(), makeTestBatch(t, n)))
	require.NoError(t, w.Close())

	assert.Len(t, f.Lines(), n)
	assert.Equal(t, int64(1), f.syncCount.Load(), "a batch must fsync exactly once, not once per record")
	assert.Equal(t, int64(1), notifies.Load(), "a batch must notify exactly once")
}

// TestWriter_WriteBatch_Empty is a no-op that touches nothing.
func TestWriter_WriteBatch_Empty(t *testing.T) {
	var notifies atomic.Int64
	w, f, _ := newFakeWriter(0, func() { notifies.Add(1) })
	require.NoError(t, w.WriteBatch(context.Background(), nil))
	require.NoError(t, w.WriteBatch(context.Background(), []*envelope.Envelope{}))
	require.NoError(t, w.Close())

	assert.Empty(t, f.Lines())
	assert.Zero(t, f.syncCount.Load())
	assert.Zero(t, notifies.Load())
}

// TestWriter_WriteBatch_ClosedErrors verifies WriteBatch after Close is rejected.
func TestWriter_WriteBatch_ClosedErrors(t *testing.T) {
	w, _, _ := newFakeWriter(1000000, nil)
	require.NoError(t, w.Close())
	require.ErrorIs(t, w.WriteBatch(context.Background(), makeTestBatch(t, 3)), ErrWriterClosed)
}

// TestWriter_WriteBatch_SegmentRolling verifies a single batch larger than one
// segment is split across segments at record boundaries (no record straddles a
// boundary) and every record remains recoverable in order.
func TestWriter_WriteBatch_SegmentRolling(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")
	const maxSeg = 200
	w, err := NewChannelWriter(channelDir, maxSeg, 1000000, nil, nil)
	require.NoError(t, err)

	const n = 20
	require.NoError(t, w.WriteBatch(context.Background(), makeTestBatch(t, n)))
	require.NoError(t, w.Close())

	segs, err := listSegments(channelDir)
	require.NoError(t, err)
	assert.Greater(t, len(segs), 1, "a batch over the segment limit must roll")
	for i := 1; i < len(segs); i++ {
		expected := segs[i-1].startOffset + segs[i-1].size
		assert.Equal(t, expected, segs[i].startOffset,
			"segment %d startOffset must equal previous segment end (no split record)", i)
	}

	lines := readAllSegments(t, channelDir)
	require.Len(t, lines, n)
	for i, line := range lines {
		assert.Equal(t, fmt.Sprintf("ord-%d", i), orderIDOf(t, line), "record %d out of order", i)
	}
}

// ---- additional fake types for doWrite / openActive error-path tests --------

// recoverableFailWriteFile is a fileWriter whose Write returns ENOSPC for the
// first `failures` calls and succeeds thereafter, used to simulate a disk
// recovering. Call heal() to drop the remaining failure count.
type recoverableFailWriteFile struct {
	mu       sync.Mutex
	failures int
	writes   atomic.Int64
	data     []byte
}

func (f *recoverableFailWriteFile) Write(b []byte) (int, error) {
	f.writes.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return 0, syscall.ENOSPC
	}
	f.data = append(f.data, b...)
	return len(b), nil
}
func (f *recoverableFailWriteFile) Sync() error  { return nil }
func (f *recoverableFailWriteFile) Close() error { return nil }
func (f *recoverableFailWriteFile) Fd() uintptr  { return 0 }
func (f *recoverableFailWriteFile) heal() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = 0
}

// alwaysFailWriteFile is a fileWriter whose Write always returns ENOSPC.
// The write count is tracked atomically for test synchronisation.
type alwaysFailWriteFile struct{ writes atomic.Int64 }

func (f *alwaysFailWriteFile) Write(_ []byte) (int, error) { f.writes.Add(1); return 0, syscall.ENOSPC }
func (f *alwaysFailWriteFile) Sync() error                 { return nil }
func (f *alwaysFailWriteFile) Close() error                { return nil }
func (f *alwaysFailWriteFile) Fd() uintptr                 { return 0 }

// syncFailFile is a fileWriter whose Write succeeds but Sync always fails.
type syncFailFile struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (f *syncFailFile) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.buf.Write(b)
}
func (f *syncFailFile) Sync() error  { return errors.New("fsync error") }
func (f *syncFailFile) Close() error { return nil }
func (f *syncFailFile) Fd() uintptr  { return 0 }

// fixedFileFactory is a segmentFactory that returns the same fileWriter for
// every openSegment and createSegment call.
type fixedFileFactory struct{ f fileWriter }

func (ff *fixedFileFactory) openSegment(_ string) (fileWriter, error)   { return ff.f, nil }
func (ff *fixedFileFactory) createSegment(_ string) (fileWriter, error) { return ff.f, nil }

// firstSuccessFactory lets only the first createSegment call succeed; all
// subsequent calls return an error. openSegment always returns the base file.
type firstSuccessFactory struct {
	base    fileWriter
	mu      sync.Mutex
	creates int
}

func (sf *firstSuccessFactory) openSegment(_ string) (fileWriter, error) { return sf.base, nil }
func (sf *firstSuccessFactory) createSegment(_ string) (fileWriter, error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	sf.creates++
	if sf.creates == 1 {
		return sf.base, nil
	}
	return nil, errors.New("no space left on device")
}

// failCreateFactory is a segmentFactory whose createSegment always returns an error.
type failCreateFactory struct{}

func (failCreateFactory) openSegment(_ string) (fileWriter, error) { return &fakeFile{}, nil }
func (failCreateFactory) createSegment(_ string) (fileWriter, error) {
	return nil, errors.New("permission denied")
}

// failOpenFactory is a segmentFactory whose openSegment always returns an error.
type failOpenFactory struct{}

func (failOpenFactory) openSegment(_ string) (fileWriter, error) {
	return nil, errors.New("permission denied")
}
func (failOpenFactory) createSegment(_ string) (fileWriter, error) { return &fakeFile{}, nil }

// ---- tests for doWrite error paths ------------------------------------------

// TestWriter_AbortOnCloseWhileRetrying verifies that Write returns a descriptive
// error when Close is called while doWrite is spinning in the retry loop.
func TestWriter_AbortOnCloseWhileRetrying(t *testing.T) {
	nf := &alwaysFailWriteFile{}
	w := newChannelWriterWithFactory("", 0, &fixedFileFactory{f: nf}, 1000000, nil, nil)

	errCh := make(chan error, 1)
	go func() { errCh <- w.Write(context.Background(), makeTestEnvelope(t, "ord")) }()

	// Wait until the retry loop has made at least one attempt.
	require.Eventually(t, func() bool { return nf.writes.Load() > 0 }, time.Second, time.Millisecond)

	require.NoError(t, w.Close())

	err := <-errCh
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write aborted")
}

// TestWriter_CtxCancelDuringRetry verifies that Write returns ctx.Err() when
// the caller's context is cancelled while the writer is spinning in the
// disk-full retry loop, without waiting for the retry to succeed.
func TestWriter_CtxCancelDuringRetry(t *testing.T) {
	nf := &alwaysFailWriteFile{}
	w := newChannelWriterWithFactory("", 0, &fixedFileFactory{f: nf}, 1000000, nil, nil)
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- w.Write(ctx, makeTestEnvelope(t, "ord")) }()

	// Wait until the retry loop has made at least one attempt, then cancel.
	require.Eventually(t, func() bool { return nf.writes.Load() > 0 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Write did not return after ctx cancellation")
	}
}

// TestWriter_CtxCancelAbandonsAndAcceptsNextWrite verifies the post-fix
// contract: after Write returns ctx.Err() during a retry loop, the writer
// goroutine has fully released the request and is ready to process subsequent
// writes. A second Write with a healthy context succeeds once the simulated
// disk recovers.
func TestWriter_CtxCancelAbandonsAndAcceptsNextWrite(t *testing.T) {
	nf := &recoverableFailWriteFile{failures: 1000}
	w := newChannelWriterWithFactory("", 0, &fixedFileFactory{f: nf}, 1000000, nil, nil)
	defer func() { _ = w.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- w.Write(ctx, makeTestEnvelope(t, "ord-1")) }()

	require.Eventually(t, func() bool { return nf.writes.Load() > 0 }, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("first Write did not return after ctx cancellation")
	}

	// Heal the "disk" and issue a second write. It must succeed promptly —
	// proving the writer goroutine processed the abandon and returned to its
	// main select, rather than leaking the first request.
	nf.heal()
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord-2")))
}

// TestWriter_SyncError verifies that a Sync() failure propagates to the caller.
func TestWriter_SyncError(t *testing.T) {
	w := newChannelWriterWithFactory("", 0, &fixedFileFactory{f: &syncFailFile{}}, 0 /* sync every write */, nil, nil)

	err := w.Write(context.Background(), makeTestEnvelope(t, "ord"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fsync channel file")

	require.NoError(t, w.Close())
}

// ---- tests for segment rolling error path -----------------------------------

// TestWriter_SegmentRollCreateError verifies that a createSegment failure during
// rolling propagates to the caller and the writer goroutine exits cleanly.
func TestWriter_SegmentRollCreateError(t *testing.T) {
	base := &fakeFile{}
	sf := &firstSuccessFactory{base: base}
	// maxSegmentBytes=1 guarantees the second write always triggers rolling.
	w := newChannelWriterWithFactory("", 1, sf, 1000000, nil, nil)

	// First write succeeds: segSize starts at 0, so the rolling condition
	// (segSize > 0) is false and rolling is skipped.
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord-1")))

	// Second write triggers rolling; the second createSegment call fails.
	err := w.Write(context.Background(), makeTestEnvelope(t, "ord-2"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create new segment")

	// Writer goroutine has exited; Close must not deadlock.
	require.NoError(t, w.Close())
}

// ---- tests for openActive error and re-open paths ---------------------------

// TestWriter_OpenActive_ExistingSegments verifies that a new writer reopens and
// appends to the most-recent segment when one already exists on disk.
func TestWriter_OpenActive_ExistingSegments(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")

	w1, err := NewChannelWriter(channelDir, 0, 1000000, nil, nil)
	require.NoError(t, err)
	require.NoError(t, w1.Write(context.Background(), makeTestEnvelope(t, "ord-1")))
	require.NoError(t, w1.Close())

	w2, err := NewChannelWriter(channelDir, 0, 1000000, nil, nil)
	require.NoError(t, err)
	require.NoError(t, w2.Write(context.Background(), makeTestEnvelope(t, "ord-2")))
	require.NoError(t, w2.Close())

	lines := readAllSegments(t, channelDir)
	assert.Len(t, lines, 2)
	for i, line := range lines {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &v), "line %d invalid JSON", i)
	}
}

// TestWriter_OpenActive_CreateError verifies that a createSegment failure during
// startup (no existing segments) is logged and Close does not deadlock.
func TestWriter_OpenActive_CreateError(t *testing.T) {
	log := &testutil.FakeLogger{}
	w := newChannelWriterWithFactory("", 0, failCreateFactory{}, 0, nil, log)

	// run() exits immediately after logging the error.
	require.NoError(t, w.Close())
	assert.True(t, log.HasError("open active segment on startup"))

	// stopCh is closed; subsequent writes must return ErrWriterClosed.
	require.ErrorIs(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")), ErrWriterClosed)
}

// TestWriter_OpenActive_OpenError verifies that an openSegment failure on an
// existing segment is logged and Close does not deadlock.
func TestWriter_OpenActive_OpenError(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "ch")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))
	// Create a segment file so listSegments returns it and openSegment is called.
	require.NoError(t, os.WriteFile(filepath.Join(channelDir, segmentName(0)), nil, 0o600))

	log := &testutil.FakeLogger{}
	w := newChannelWriterWithFactory(channelDir, 0, failOpenFactory{}, 0, nil, log)

	require.NoError(t, w.Close())
	assert.True(t, log.HasError("open active segment on startup"))

	require.ErrorIs(t, w.Write(context.Background(), makeTestEnvelope(t, "ord")), ErrWriterClosed)
}

// TestWriter_OpenActive_TruncatesPartialTrailing verifies that when the active
// segment ends in non-newline-terminated bytes (the signature of a crash
// mid-write), the writer truncates them on startup so subsequent appends
// produce valid records. Without this, the next message would concatenate
// with the garbage and fail to unmarshal in the subscriber.
func TestWriter_OpenActive_TruncatesPartialTrailing(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")

	// Stage 1: write N valid lines via a normal ChannelWriter and close it cleanly.
	w1, err := NewChannelWriter(channelDir, 0, 1000000, nil, nil)
	require.NoError(t, err)
	const validLines = 5
	for i := 0; i < validLines; i++ {
		require.NoError(t, w1.Write(context.Background(), makeTestEnvelope(t, fmt.Sprintf("ord-%d", i))))
	}
	require.NoError(t, w1.Close())

	// Stage 2: simulate a crash mid-write by appending garbage bytes with no
	// trailing newline directly to the segment file.
	segPath := filepath.Join(channelDir, segmentName(0))
	cleanInfo, err := os.Stat(segPath)
	require.NoError(t, err)
	cleanSize := cleanInfo.Size()

	const garbage = `{"id":"partial","channel":"orders","payload":{"data":"trunc`
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0) //nolint:gosec // test file: segPath is test-controlled
	require.NoError(t, err)
	_, err = f.Write([]byte(garbage))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Sanity check: the file now ends without a newline.
	tail := make([]byte, 1)
	rf, err := os.Open(segPath) //nolint:gosec // test file: segPath is test-controlled
	require.NoError(t, err)
	dirty, err := rf.Stat()
	require.NoError(t, err)
	require.Equal(t, cleanSize+int64(len(garbage)), dirty.Size())
	_, err = rf.ReadAt(tail, dirty.Size()-1)
	require.NoError(t, err)
	require.NotEqual(t, byte('\n'), tail[0])
	require.NoError(t, rf.Close())

	// Stage 3: open a new ChannelWriter on the same directory. Startup must
	// detect the partial trailing bytes and truncate them.
	log := &testutil.FakeLogger{}
	w2, err := NewChannelWriter(channelDir, 0, 1000000, nil, log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	require.Eventually(t, func() bool {
		info, err := os.Stat(segPath)
		return err == nil && info.Size() == cleanSize
	}, time.Second, 10*time.Millisecond, "writer did not truncate partial trailing bytes")

	assert.True(t, log.HasWarn("recovered partial trailing bytes after crash"),
		"expected recovery warning in logs")

	// Stage 4: write one more message. It must append cleanly and parse as
	// valid JSON — the proof that the file invariant has been restored.
	require.NoError(t, w2.Write(context.Background(), makeTestEnvelope(t, "ord-post-crash")))
	require.NoError(t, w2.Close())

	lines := readAllSegments(t, channelDir)
	assert.Len(t, lines, validLines+1)
	for i, line := range lines {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &v), "line %d invalid JSON: %q", i, line)
	}
}

// TestWriter_OpenActive_TruncatesFullyCorruptSegment verifies the edge case
// where the segment file contains no '\n' at all (e.g. the very first write
// crashed). The writer should truncate the file to zero so subsequent writes
// produce a clean log.
func TestWriter_OpenActive_TruncatesFullyCorruptSegment(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	// Create a segment file filled with non-newline bytes.
	segPath := filepath.Join(channelDir, segmentName(0))
	require.NoError(t, os.WriteFile(segPath, []byte("partial-garbage-no-newline-ever"), 0o600))

	log := &testutil.FakeLogger{}
	w, err := NewChannelWriter(channelDir, 0, 1000000, nil, log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	require.Eventually(t, func() bool {
		info, err := os.Stat(segPath)
		return err == nil && info.Size() == 0
	}, time.Second, 10*time.Millisecond, "writer did not truncate fully-corrupt segment")

	assert.True(t, log.HasWarn("recovered fully-corrupt segment after crash"),
		"expected fully-corrupt recovery warning in logs")

	// Subsequent writes succeed and produce a valid record.
	require.NoError(t, w.Write(context.Background(), makeTestEnvelope(t, "ord-post-crash")))
	require.NoError(t, w.Close())

	lines := readAllSegments(t, channelDir)
	require.Len(t, lines, 1)
	var v map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &v))
}

// TestWriter_OpenActive_CleanSegmentNoTruncation verifies that a segment
// already ending in '\n' (the normal case) is left untouched and no recovery
// warning is logged.
func TestWriter_OpenActive_CleanSegmentNoTruncation(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")

	w1, err := NewChannelWriter(channelDir, 0, 1000000, nil, nil)
	require.NoError(t, err)
	require.NoError(t, w1.Write(context.Background(), makeTestEnvelope(t, "ord-1")))
	require.NoError(t, w1.Close())

	segPath := filepath.Join(channelDir, segmentName(0))
	beforeInfo, err := os.Stat(segPath)
	require.NoError(t, err)

	log := &testutil.FakeLogger{}
	w2, err := NewChannelWriter(channelDir, 0, 1000000, nil, log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	require.Eventually(t, func() bool {
		info, err := os.Stat(segPath)
		return err == nil && info.Size() == beforeInfo.Size()
	}, time.Second, 10*time.Millisecond)

	assert.False(t, log.HasWarn("recovered partial trailing bytes"),
		"clean segment must not trigger recovery warning")
	assert.False(t, log.HasWarn("recovered fully-corrupt segment"),
		"clean segment must not trigger fully-corrupt recovery warning")
}
