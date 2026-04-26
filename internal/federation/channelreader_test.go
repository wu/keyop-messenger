package federation

import (
	"fmt"
	"os"
	"path/filepath"
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

// makeEnvelope is a test helper that creates a simple envelope.
func makeEnvelope(t *testing.T, channel, id string) envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "test-origin", "test.Type", map[string]any{"id": id})
	require.NoError(t, err)
	return env
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
	cr, err := newChannelReader("peer1", "test-ch", channelDir, offsetDir, 65536, requestCh, log)
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
	cr, err := newChannelReader("peer1", "events", channelDir, offsetDir, 65536, requestCh, log)
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

	// Give the reader time to persist the offset.
	time.Sleep(50 * time.Millisecond)

	offsetPath := filepath.Join(offsetDir, "fed-peer1.offset")
	offset, err := storage.ReadOffset(offsetPath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), offset)
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
	cr, err := newChannelReader("peer1", "big", channelDir, offsetDir, 0 /* placeholder */, requestCh, log)
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
	cr, err := newChannelReader("peer1", "wake", channelDir, offsetDir, 65536, requestCh, log)
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
	cr, err := newChannelReader("peer1", "resume", channelDir, offsetDir, 65536, requestCh, log)
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

// TestChannelReader_MultipleNotificationsCoalesced verifies that rapid
// back-to-back notify() calls do not cause extra reads.
func TestChannelReader_MultipleNotificationsCoalesced(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "channels", "coalesce")
	offsetDir := filepath.Join(dir, "subscribers", "coalesce")
	log := &testutil.FakeLogger{}

	requestCh := make(chan sendReq, 4)
	cr, err := newChannelReader("peer1", "coalesce", channelDir, offsetDir, 65536, requestCh, log)
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
	cr, err := newChannelReader("peer1", "stopchan", channelDir, offsetDir, 65536, requestCh, log)
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
	cr, err := newChannelReader("peer1", "concurrent", channelDir, offsetDir, 65536, requestCh, log)
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
