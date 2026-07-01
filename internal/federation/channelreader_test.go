package federation

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// writeTestSegment writes JSONL envelopes to a segment file in channelDir and
// returns the raw bytes written (including trailing newlines).
func writeTestSegment(t *testing.T, channelDir string, startOffset int64, envs []envelope.Envelope) []byte {
	t.Helper()
	require.NoError(t, os.MkdirAll(channelDir, 0o750))
	name := fmt.Sprintf("%020d.jsonl", startOffset)
	path := filepath.Join(channelDir, name)
	var data []byte
	for _, env := range envs {
		b, err := envelope.Marshal(env)
		require.NoError(t, err)
		data = append(data, b...)
		data = append(data, '\n')
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return data
}

// TestNewChannelReader_SanitizesPeerNameForOffsetPath verifies that a peerName
// (on the hub side, the peer's certificate CN) containing path separators cannot
// write its offset file outside offsetDir.
func TestNewChannelReader_SanitizesPeerNameForOffsetPath(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "ch")
	offsetDir := filepath.Join(dir, "subscribers", "ch")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 1)
	// "../../evil" would otherwise escape offsetDir entirely. The reader is not
	// started (close() would block waiting on a goroutine that never ran); the
	// offset file is written during construction, which is all this test checks.
	_, err := newChannelReader("../../evil", "ch", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)

	// No offset file may appear outside offsetDir.
	_, statErr := os.Stat(filepath.Join(dir, "fed-evil.offset"))
	assert.True(t, os.IsNotExist(statErr), "offset must not escape offsetDir")

	// The sanitized offset file lives inside offsetDir.
	want := filepath.Join(offsetDir, "fed-"+storage.SanitizeForFilename("../../evil")+".offset")
	_, statErr = os.Stat(want)
	require.NoError(t, statErr, "sanitized offset file must exist inside offsetDir")
}

// makeEnvelope is a test helper that creates a simple envelope.
func makeEnvelope(t *testing.T, channel, id string) envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "test-origin", "test.Type", map[string]any{"id": id})
	require.NoError(t, err)
	return env
}

// makeBigEnvelope creates an envelope whose marshalled JSONL line is at least
// minBytes long, used to exercise the oversized-record handling.
func makeBigEnvelope(t *testing.T, channel string, minBytes int) envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "test-origin", "test.Type",
		map[string]any{"id": strings.Repeat("x", minBytes)})
	require.NoError(t, err)
	return env
}

// drainUntilOffset acks every batch from requestCh until the reader reaches
// wantOffset, returning the total number of records delivered. It fails if the
// reader loops without making progress, which is the bug this guards against.
func drainUntilOffset(t *testing.T, requestCh <-chan sendReq, wantOffset int64) int {
	t.Helper()
	deadline := time.After(5 * time.Second)
	delivered := 0
	for {
		select {
		case req := <-requestCh:
			delivered += len(req.rawLines)
			off := req.newOffset
			close(req.doneCh)
			if off >= wantOffset {
				return delivered
			}
		case <-deadline:
			t.Fatalf("reader did not reach offset %d (delivered=%d)", wantOffset, delivered)
		}
	}
}

// TestChannelReader_SkipsOversizedRecord verifies that a record larger than the
// per-record cap (but small enough to scan) is dropped and surrounding records
// are still delivered — the reader must not loop forever on the poison record.
// Limits are shrunk so the oversized record is a few KiB, not megabytes.
func TestChannelReader_SkipsOversizedRecord(t *testing.T) {
	setTestLimits(t, 2*1024, 4*1024) // cap 2 KiB, frame limit 6 KiB
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "big")
	offsetDir := filepath.Join(dir, "subscribers", "big")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "big", channelDir, offsetDir, "fed-", 2*1024, requestCh, nil, log)
	require.NoError(t, err)

	small1 := makeEnvelope(t, "big", "small1")
	// > maxRecordBytes (2 KiB) but < the scan buffer, so it is read then dropped in-loop.
	oversized := makeBigEnvelope(t, "big", 3*1024)
	small2 := makeEnvelope(t, "big", "small2")
	data := writeTestSegment(t, channelDir, 0, []envelope.Envelope{small1, oversized, small2})

	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	delivered := drainUntilOffset(t, requestCh, int64(len(data)))
	assert.Equal(t, 2, delivered, "both small records delivered, oversized dropped")

	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	require.Eventually(t, func() bool {
		off, err := storage.ReadOffset(offsetPath)
		return err == nil && off == int64(len(data))
	}, 2*time.Second, 10*time.Millisecond, "offset should advance past the oversized record")
}

// TestChannelReader_SkipsUnscannableRecord verifies the bufio.ErrTooLong path:
// a record larger than the scan buffer is skipped via offsetPastNextLine so the
// reader still makes forward progress.
func TestChannelReader_SkipsUnscannableRecord(t *testing.T) {
	setTestLimits(t, 2*1024, 512) // frame limit 2.5 KiB → scan buffer 2.5 KiB
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "huge")
	offsetDir := filepath.Join(dir, "subscribers", "huge")
	log := &testutil.FakeLogger{}

	const maxBatch = 2 * 1024
	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "huge", channelDir, offsetDir, "fed-", maxBatch, requestCh, nil, log)
	require.NoError(t, err)

	small1 := makeEnvelope(t, "huge", "small1")
	// Larger than the scan buffer (grpcMessageLimit) → triggers bufio.ErrTooLong.
	unscannable := makeBigEnvelope(t, "huge", grpcMessageLimit(maxBatch)+1024)
	small2 := makeEnvelope(t, "huge", "small2")
	data := writeTestSegment(t, channelDir, 0, []envelope.Envelope{small1, unscannable, small2})

	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	delivered := drainUntilOffset(t, requestCh, int64(len(data)))
	assert.Equal(t, 2, delivered, "both small records delivered, unscannable record dropped")

	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	require.Eventually(t, func() bool {
		off, err := storage.ReadOffset(offsetPath)
		return err == nil && off == int64(len(data))
	}, 2*time.Second, 10*time.Millisecond, "offset should advance past the unscannable record")
}

// TestChannelReader_SkipsLoneOversizedRecord covers the case where a batch
// contains nothing but a dropped record: readBatch returns no rawLines but an
// advanced offset, and drainAndSend must still persist that offset so the reader
// does not re-scan (and re-drop) the same record on every notification.
func TestChannelReader_SkipsLoneOversizedRecord(t *testing.T) {
	setTestLimits(t, 2*1024, 4*1024)
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "lone")
	offsetDir := filepath.Join(dir, "subscribers", "lone")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "lone", channelDir, offsetDir, "fed-", 2*1024, requestCh, nil, log)
	require.NoError(t, err)

	// A single oversized record, nothing else.
	oversized := makeBigEnvelope(t, "lone", 3*1024)
	data := writeTestSegment(t, channelDir, 0, []envelope.Envelope{oversized})

	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	// No batch should ever be delivered — the only record was dropped.
	select {
	case req := <-requestCh:
		close(req.doneCh)
		t.Fatalf("unexpected batch with %d records; lone oversized record must be dropped", len(req.rawLines))
	case <-time.After(200 * time.Millisecond):
	}

	// ...but the offset must still advance past it so it is not re-scanned.
	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	require.Eventually(t, func() bool {
		off, err := storage.ReadOffset(offsetPath)
		return err == nil && off == int64(len(data))
	}, 2*time.Second, 10*time.Millisecond, "offset must advance even when the batch is empty")
}

// TestChannelReader_NewSubscriber_StartsAtEnd verifies that a brand-new reader
// (no existing offset file) starts at the current end of the channel and does
// not replay history.
func TestChannelReader_NewSubscriber_StartsAtEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "test-ch")
	offsetDir := filepath.Join(dir, "subscribers", "test-ch")
	log := &testutil.FakeLogger{}

	// Write some messages before the reader is created.
	env1 := makeEnvelope(t, "test-ch", "pre1")
	env2 := makeEnvelope(t, "test-ch", "pre2")
	writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1, env2})

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "test-ch", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)

	// Offset file must exist and point to the end of the channel.
	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	require.True(t, storage.OffsetFileExists(offsetPath))
	offset, err := storage.ReadOffset(offsetPath)
	require.NoError(t, err)
	assert.Greater(t, offset, int64(0), "new subscriber should start past existing data")

	cr.start()
	t.Cleanup(cr.close)

	// No notification → no batch should be sent.
	select {
	case req := <-requestCh:
		t.Fatalf("unexpected batch: %v lines", len(req.rawLines))
	case <-time.After(50 * time.Millisecond):
		// expected: nothing to deliver
	}
}

// TestChannelReader_DropsEchoSendSide verifies the send-side loop guard: a
// record whose path vector already contains the destination's identity is not
// forwarded (never crosses the wire), while records that have not visited the
// destination are delivered. The offset still advances past the dropped record.
func TestChannelReader_DropsEchoSendSide(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "metrics")
	offsetDir := filepath.Join(dir, "subscribers", "metrics")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 4)
	// Destination is "hub-a": a record already routed through hub-a is an echo.
	cr, err := newChannelReader("peer-hub-a", "metrics", channelDir, offsetDir, "fedout-",
		65536, requestCh, func() string { return "hub-a" }, log)
	require.NoError(t, err)

	// echo has hub-a in its route → must be dropped send-side.
	echo := makeEnvelope(t, "metrics", "echo")
	echo.AppendRoute("hub-a")
	// local was published here and has never visited hub-a → must be delivered.
	local := makeEnvelope(t, "metrics", "local")
	data := writeTestSegment(t, channelDir, 0, []envelope.Envelope{echo, local})

	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	var req sendReq
	select {
	case req = <-requestCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch")
	}
	require.Len(t, req.rawLines, 1, "only the non-echo record should be forwarded")
	assert.Contains(t, string(req.rawLines[0]), `"local"`)
	assert.NotContains(t, string(req.rawLines[0]), `"echo"`)
	// The offset must advance past both records (the dropped echo included).
	assert.Equal(t, int64(len(data)), req.newOffset)
	close(req.doneCh)
}

// TestChannelReader_DeliveryAndOffsetPersistence verifies that after a batch is
// acked the offset file is updated to the end of that batch.
func TestChannelReader_DeliveryAndOffsetPersistence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "events")
	offsetDir := filepath.Join(dir, "subscribers", "events")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 4)
	// Create reader before writing any data → starts at offset 0.
	cr, err := newChannelReader("peer1", "events", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)

	// Write messages after the reader is created.
	env1 := makeEnvelope(t, "events", "msg1")
	env2 := makeEnvelope(t, "events", "msg2")
	data := writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1, env2})

	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	// Collect and ack the batch.
	var req sendReq
	select {
	case req = <-requestCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch")
	}
	assert.Len(t, req.rawLines, 2)
	assert.Equal(t, "events", req.channel)
	assert.Equal(t, int64(len(data)), req.newOffset)
	close(req.doneCh)

	// Poll until the channelreader goroutine persists the offset (WriteOffset
	// calls fsync before the atomic rename, which can be slow on CI).
	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	want := int64(len(data))
	deadline := time.Now().Add(5 * time.Second)
	var offset int64
	for time.Now().Before(deadline) {
		var readErr error
		offset, readErr = storage.ReadOffset(offsetPath)
		if readErr == nil && offset == want {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.Equal(t, want, offset)
}

// TestChannelReader_BatchSizeLimit verifies that large messages are split into
// multiple batches when maxBatchBytes is set.
func TestChannelReader_BatchSizeLimit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "big")
	offsetDir := filepath.Join(dir, "subscribers", "big")
	log := &testutil.FakeLogger{}

	// Create reader BEFORE writing data so it starts at offset 0.
	// (channelDir does not exist yet → listChannelSegments returns nil → offset=0)
	requestCh := make(chan sendReq, 10)
	cr, err := newChannelReader("peer1", "big", channelDir, offsetDir, "fed-", 0 /* placeholder */, requestCh, nil, log)
	require.NoError(t, err)

	// Write 5 messages. Compute the per-line byte size from the actual output.
	var envs []envelope.Envelope
	for i := 0; i < 5; i++ {
		e := makeEnvelope(t, "big", fmt.Sprintf("msg%d", i))
		envs = append(envs, e)
	}
	data := writeTestSegment(t, channelDir, 0, envs)
	// Set maxBatchBytes to fit exactly 2 lines (each line is the same size since
	// the envelope JSON is uniform except for the id, which we accept as ~equal).
	lineSize := len(data) / 5
	cr.maxBatchBytes = lineSize*2 + 1

	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	// Collect all batches.
	var batches []sendReq
	deadline := time.After(2 * time.Second)
	for len(batches) < 3 { // 5 messages at 2-per-batch → 3 batches (2+2+1)
		select {
		case req := <-requestCh:
			batches = append(batches, req)
			close(req.doneCh)
		case <-deadline:
			t.Fatalf("timed out; got %d batches", len(batches))
		}
	}

	total := 0
	for _, b := range batches {
		total += len(b.rawLines)
	}
	assert.Equal(t, 5, total, "all 5 messages should be delivered across batches")
	assert.GreaterOrEqual(t, len(batches), 2, "should require at least 2 batches")
}

// TestChannelReader_NotificationWakeup verifies that data written after the
// reader starts is delivered when notify() is called.
func TestChannelReader_NotificationWakeup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "wake")
	offsetDir := filepath.Join(dir, "subscribers", "wake")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "wake", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)
	cr.start()
	t.Cleanup(cr.close)

	// No data yet — notify should produce nothing.
	cr.notify()
	select {
	case <-requestCh:
		t.Fatal("unexpected batch before data written")
	case <-time.After(50 * time.Millisecond):
	}

	// Now write data and notify.
	env1 := makeEnvelope(t, "wake", "late1")
	writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1})
	cr.notify()

	select {
	case req := <-requestCh:
		assert.Len(t, req.rawLines, 1)
		close(req.doneCh)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch after notification")
	}
}

// TestChannelReader_ResumeFromOffset verifies that a reader resumes from a
// stored offset on construction (simulating a reconnect scenario).
func TestChannelReader_ResumeFromOffset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "resume")
	offsetDir := filepath.Join(dir, "subscribers", "resume")
	log := &testutil.FakeLogger{}
	require.NoError(t, os.MkdirAll(offsetDir, 0o750))
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	// Write 3 messages.
	env1 := makeEnvelope(t, "resume", "m1")
	env2 := makeEnvelope(t, "resume", "m2")
	env3 := makeEnvelope(t, "resume", "m3")
	data := writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1, env2, env3})

	// Compute byte offset after the first message.
	b1, err := envelope.Marshal(env1)
	require.NoError(t, err)
	offsetAfterMsg1 := int64(len(b1)) + 1 // +1 for '\n'

	// Pre-write an offset file pointing past message 1 only.
	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	require.NoError(t, storage.WriteOffset(offsetPath, offsetAfterMsg1))

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "resume", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)
	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	select {
	case req := <-requestCh:
		// Should deliver only messages 2 and 3 (not 1, which was already acked).
		assert.Len(t, req.rawLines, 2)
		assert.Equal(t, int64(len(data)), req.newOffset)
		close(req.doneCh)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch")
	}
}

// TestChannelReader_UndercutResumesAtOldestAvailable verifies that when retention
// compaction has dropped segments below a (disconnected) reader's stored offset,
// the reader resumes at the oldest still-available message rather than failing or
// stalling. This is the reconnect half of the old-log-file force-eviction
// contract: the cap drops segments a disconnected client never consumed, and on
// reconnect the client picks up at the oldest message still on disk.
func TestChannelReader_UndercutResumesAtOldestAvailable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "undercut")
	offsetDir := filepath.Join(dir, "subscribers", "undercut")
	log := &testutil.FakeLogger{}
	require.NoError(t, os.MkdirAll(offsetDir, 0o750))
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	// Simulate that compaction force-evicted everything below offset 500: the only
	// surviving segment starts at 500.
	const earliest = int64(500)
	m1 := makeEnvelope(t, "undercut", "m1")
	m2 := makeEnvelope(t, "undercut", "m2")
	data := writeTestSegment(t, channelDir, earliest, []envelope.Envelope{m1, m2})

	// The reader's stored offset (100) is below the earliest surviving segment —
	// it was undercut while the client was disconnected.
	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	require.NoError(t, storage.WriteOffset(offsetPath, 100))

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "undercut", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)
	cr.start()
	t.Cleanup(cr.close)
	cr.notify()

	select {
	case req := <-requestCh:
		assert.Len(t, req.rawLines, 2, "both surviving records delivered from the oldest available")
		assert.Equal(t, earliest+int64(len(data)), req.newOffset,
			"offset resumes at oldest available; the dropped [100,500) gap is not replayed")
		close(req.doneCh)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch")
	}
}

// TestChannelReader_MultipleNotificationsCoalesced verifies that rapid
// back-to-back notify() calls do not cause extra reads.
func TestChannelReader_MultipleNotificationsCoalesced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "coalesce")
	offsetDir := filepath.Join(dir, "subscribers", "coalesce")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "coalesce", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)

	env1 := makeEnvelope(t, "coalesce", "m1")
	writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1})

	cr.start()
	t.Cleanup(cr.close)

	// Send many notifications rapidly.
	for i := 0; i < 10; i++ {
		cr.notify()
	}

	// We should get exactly one batch (coalesced).
	select {
	case req := <-requestCh:
		assert.Len(t, req.rawLines, 1)
		close(req.doneCh)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch")
	}

	// No second batch should arrive.
	select {
	case <-requestCh:
		// This could legitimately happen if the reader loops and picks up the
		// same data again, but newOffset will be at end so rawLines will be empty.
		// The reader returns immediately on empty, so no second request arrives.
		t.Error("unexpected second batch")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

// TestChannelReader_Close_StopsGoroutine verifies that close() terminates the
// reader goroutine promptly even when blocked waiting for an ack.
func TestChannelReader_Close_StopsGoroutine(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "stopchan")
	offsetDir := filepath.Join(dir, "subscribers", "stopchan")
	log := &testutil.FakeLogger{}

	// Use an unbuffered requestCh that no one reads from; the reader will block.
	requestCh := make(chan sendReq)
	cr, err := newChannelReader("peer1", "stopchan", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)

	env1 := makeEnvelope(t, "stopchan", "m1")
	writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1})

	cr.start()
	cr.notify()

	// Give it a moment to block on requestCh <- req.
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	cr.close()
	assert.Less(t, time.Since(start), 2*time.Second, "close() should not hang")
}

// TestListChannelSegments_Empty returns nil for a non-existent directory.
func TestListChannelSegments_Empty(t *testing.T) {
	t.Parallel()
	segs, err := listChannelSegments("/tmp/does-not-exist-channelreader-test")
	assert.NoError(t, err)
	assert.Nil(t, segs)
}

// TestListChannelSegments_IgnoresNonSegmentFiles skips files without .jsonl extension.
func TestListChannelSegments_IgnoresNonSegmentFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.txt"), []byte("hi"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00000000000000000000.jsonl"), []byte{}, 0o600))

	segs, err := listChannelSegments(dir)
	assert.NoError(t, err)
	assert.Len(t, segs, 1)
}

// TestChannelReader_ConcurrentNotify verifies the reader is safe to notify
// from multiple goroutines simultaneously.
func TestChannelReader_ConcurrentNotify(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "concurrent")
	offsetDir := filepath.Join(dir, "subscribers", "concurrent")
	log := &testutil.FakeLogger{}

	env1 := makeEnvelope(t, "concurrent", "m1")
	writeTestSegment(t, channelDir, 0, []envelope.Envelope{env1})

	requestCh := make(chan sendReq, 8)
	cr, err := newChannelReader("peer1", "concurrent", channelDir, offsetDir, "fed-", 65536, requestCh, nil, log)
	require.NoError(t, err)
	cr.start()
	t.Cleanup(cr.close)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cr.notify()
		}()
	}
	wg.Wait()

	// Ack any batches that arrive.
	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case req := <-requestCh:
			close(req.doneCh)
		case <-deadline:
			return
		}
	}
}
