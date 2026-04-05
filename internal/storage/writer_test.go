package storage

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
func newFakeWriter(policy SyncPolicy, syncInterval time.Duration, notifyFn func()) (*channelWriter, *fakeFile, *testutil.FakeLogger) {
	f := &fakeFile{}
	log := &testutil.FakeLogger{}
	sf := &fakeSegmentFactory{f: f}
	w := newChannelWriterWithFactory("", 0, sf, policy, syncInterval, notifyFn, log)
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
		f.Close()
	}
	return lines
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
	assert.Equal(t, int64(n), f.syncCount.Load(), "SyncPolicyAlways must Sync once per write")
}

func TestWriter_SyncPolicyPeriodic(t *testing.T) {
	const interval = 50 * time.Millisecond
	w, f, _ := newFakeWriter(SyncPolicyPeriodic, interval, nil)
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	assert.Zero(t, f.syncCount.Load(), "SyncPolicyPeriodic must not Sync on each write")
	time.Sleep(3 * interval)
	assert.GreaterOrEqual(t, f.syncCount.Load(), int64(1), "SyncPolicyPeriodic must Sync on tick")
	require.NoError(t, w.Close())
}

func TestWriter_SyncPolicyNone_NoSync(t *testing.T) {
	w, f, _ := newFakeWriter(SyncPolicyNone, 0, nil)
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Close())
	assert.Zero(t, f.syncCount.Load(), "SyncPolicyNone must never Sync")
}

func TestWriter_Backpressure(t *testing.T) {
	const failFirst = 5
	ff := &fakeFile{failFirst: failFirst}
	sf := &fakeSegmentFactory{f: ff}
	w := newChannelWriterWithFactory("", 0, sf, SyncPolicyNone, 0, nil, nil)
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord-1")))
	require.NoError(t, w.Close())

	lines := ff.Lines()
	require.Len(t, lines, 1, "record must appear after retries")
	var v map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &v))
	assert.Equal(t, failFirst, ff.FailsDone())
}

func TestWriter_NotifyFn(t *testing.T) {
	var calls atomic.Int64
	w, _, _ := newFakeWriter(SyncPolicyNone, 0, func() { calls.Add(1) })
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
	require.NoError(t, w.Close())
	assert.Equal(t, int64(2), calls.Load(), "notifyFn must be called once per write")
}

func TestWriter_Close_SubsequentWriteErrors(t *testing.T) {
	w, _, _ := newFakeWriter(SyncPolicyNone, 0, nil)
	require.NoError(t, w.Close())
	require.ErrorIs(t, w.Write(makeTestEnvelope(t, "ord")), ErrWriterClosed)
}

func TestWriter_Close_Idempotent(t *testing.T) {
	w, _, _ := newFakeWriter(SyncPolicyNone, 0, nil)
	assert.NoError(t, w.Close())
	assert.NoError(t, w.Close())
}

func TestWriter_RealFile(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "orders")
	w, err := NewChannelWriter(channelDir, 0, SyncPolicyNone, 0, nil, nil)
	require.NoError(t, err)

	const n = 100
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
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
	w, err := NewChannelWriter(channelDir, 0, SyncPolicyNone, 0, nil, nil)
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
	w, err := NewChannelWriter(channelDir, maxSeg, SyncPolicyNone, 0, nil, nil)
	require.NoError(t, err)

	const n = 20
	for i := 0; i < n; i++ {
		require.NoError(t, w.Write(makeTestEnvelope(t, "ord")))
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
