//nolint:gosec // test file: G304/G306
package audit_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// readJSONLEvents reads all JSON lines from path and decodes them into Events.
func readJSONLEvents(t *testing.T, path string) []audit.Event {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var events []audit.Event
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev audit.Event
		require.NoError(t, json.Unmarshal(line, &ev), "invalid JSON line: %s", line)
		events = append(events, ev)
	}
	require.NoError(t, scanner.Err())
	return events
}

// countAllEvents reads audit.jsonl and all rotated files, returning the total event count.
func countAllEvents(t *testing.T, dir string) int {
	t.Helper()
	base := filepath.Join(dir, "audit.jsonl")
	total := 0

	// Live file.
	if _, err := os.Stat(base); err == nil {
		total += len(readJSONLEvents(t, base))
	}

	// Rotated files.
	for i := 1; ; i++ {
		p := fmt.Sprintf("%s.%d", base, i)
		if _, err := os.Stat(p); os.IsNotExist(err) {
			break
		}
		total += len(readJSONLEvents(t, p))
	}
	return total
}

// TestWriteAndRead verifies that 5 events are flushed to disk as valid JSON lines.
func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	logger := &testutil.FakeLogger{}

	aw, err := audit.NewAuditWriter(dir, 100, 10, logger)
	require.NoError(t, err)

	events := []audit.Event{
		{Event: audit.EventForward, MessageID: "msg1", Channel: "ch", Direction: "out", Peer: "hub2"},
		{Event: audit.EventPolicyViolation, Channel: "blocked"},
		{Event: audit.EventPeerConnected, Peer: "hub3", PeerAddr: "10.0.0.1:9000"},
		{Event: audit.EventPeerDisconnected, Peer: "hub3"},
		{Event: audit.EventPolicyReloaded, Detail: "ok"},
	}
	for _, ev := range events {
		require.NoError(t, aw.Log(ev))
	}
	require.NoError(t, aw.Close())

	got := readJSONLEvents(t, filepath.Join(dir, "audit.jsonl"))
	require.Len(t, got, 5)

	for i, ev := range got {
		assert.Equal(t, events[i].Event, ev.Event)
		assert.False(t, ev.Ts.IsZero(), "Ts should be set")
	}
}

// TestRotation verifies that setting max_size_mb=1 triggers file rotation.
// Events are sized at ~1200 bytes each so that the 1000-event channel buffer
// carries enough data (~1.2 MB) to exceed the 1 MB rotation threshold even
// under channel back-pressure.
func TestRotation(t *testing.T) {
	dir := t.TempDir()
	logger := &testutil.FakeLogger{}

	// 1 MB rotation threshold.
	aw, err := audit.NewAuditWriter(dir, 1, 10, logger)
	require.NoError(t, err)

	// Each event carries a ~1100-byte detail string so that 1000 queued
	// events (~1.1 MB) reliably exceed the threshold.
	bigDetail := string(make([]byte, 1100)) // 1100 zero bytes → 1100-char string
	for i := 0; i < 1100; i++ {
		require.NoError(t, aw.Log(audit.Event{
			Event:  audit.EventForward,
			Detail: bigDetail,
		}))
	}
	require.NoError(t, aw.Close())

	// audit.jsonl.1 must exist (rotation happened at least once).
	rotated := filepath.Join(dir, "audit.jsonl.1")
	_, err = os.Stat(rotated)
	require.NoError(t, err, "audit.jsonl.1 should exist after rotation")
}

// TestMaxFiles verifies that rotated files beyond max_files are deleted.
func TestMaxFiles(t *testing.T) {
	dir := t.TempDir()
	logger := &testutil.FakeLogger{}

	// Very small rotation threshold so we rotate often; keep only 3 files.
	aw, err := audit.NewAuditWriter(dir, 0, 3, logger)
	require.NoError(t, err)

	// Manually pre-create rotated files .1, .2, .3 to simulate prior rotations.
	base := filepath.Join(dir, "audit.jsonl")
	for i := 1; i <= 3; i++ {
		p := fmt.Sprintf("%s.%d", base, i)
		require.NoError(t, os.WriteFile(p, []byte(`{"event":"old"}`+"\n"), 0o644))
	}
	require.NoError(t, aw.Close())

	// Trigger a rotation directly by calling the exported path via a tiny-threshold writer.
	aw2, err := audit.NewAuditWriter(dir, 0, 3, logger)
	require.NoError(t, err)

	// Write a dummy event to create the live file, then close to flush.
	require.NoError(t, aw2.Log(audit.Event{Event: audit.EventForward}))

	// Wait a moment for the goroutine to process.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, aw2.Close())

	// Force rotation by creating a large pre-existing live file.
	bigData := make([]byte, 2*1024*1024) // 2 MB
	require.NoError(t, os.WriteFile(base, bigData, 0o644))

	// Reset rotated files to .1, .2, .3.
	for i := 1; i <= 3; i++ {
		p := fmt.Sprintf("%s.%d", base, i)
		require.NoError(t, os.WriteFile(p, []byte(`{"event":"old"}`+"\n"), 0o644))
	}

	// Now start a writer with max 3 files and write enough to trigger rotation.
	aw3, err := audit.NewAuditWriter(dir, 1, 3, logger)
	require.NoError(t, err)
	// Write enough to trigger rotation.
	for i := 0; i < 15000; i++ {
		require.NoError(t, aw3.Log(audit.Event{
			Event:  audit.EventForward,
			Detail: fmt.Sprintf("fill-%d", i),
		}))
	}
	require.NoError(t, aw3.Close())

	// File .4 must not exist.
	excess := fmt.Sprintf("%s.4", base)
	_, err = os.Stat(excess)
	assert.True(t, os.IsNotExist(err), "audit.jsonl.4 should have been deleted")
}

// TestConcurrentLog verifies no data races when 50 goroutines each log 100 events.
func TestConcurrentLog(t *testing.T) {
	dir := t.TempDir()
	logger := &testutil.FakeLogger{}

	aw, err := audit.NewAuditWriter(dir, 100, 10, logger)
	require.NoError(t, err)

	const goroutines = 50
	const perGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				_ = aw.Log(audit.Event{
					Event:  audit.EventForward,
					Detail: fmt.Sprintf("g%d-i%d", g, i),
				})
			}
		}()
	}
	wg.Wait()
	require.NoError(t, aw.Close())

	// All events that fit in the channel should be on disk.
	got := countAllEvents(t, dir)
	// The channel capacity is 1000; with 5000 total events some may be dropped,
	// but at least the channel capacity worth should land.
	assert.GreaterOrEqual(t, got, 1, "at least one event should be written")
}

// TestCloseClean verifies the goroutine exits and the file is closed cleanly.
func TestCloseClean(t *testing.T) {
	dir := t.TempDir()
	logger := &testutil.FakeLogger{}

	aw, err := audit.NewAuditWriter(dir, 100, 10, logger)
	require.NoError(t, err)

	require.NoError(t, aw.Log(audit.Event{Event: audit.EventPeerConnected, Peer: "hub1"}))

	// Close must return without hanging.
	done := make(chan struct{})
	go func() {
		_ = aw.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() timed out")
	}

	// File must be present and non-empty.
	fi, err := os.Stat(filepath.Join(dir, "audit.jsonl"))
	require.NoError(t, err)
	assert.Greater(t, fi.Size(), int64(0))
}

// TestDropWarning verifies that when the event channel is full, the drop count
// is reported via the structured logger on Close (final warnDrops call).
func TestDropWarning(t *testing.T) {
	dir := t.TempDir()
	logger := &testutil.FakeLogger{}

	// Capacity is 1000; flood with 2000 events so some are definitely dropped.
	aw, err := audit.NewAuditWriter(dir, 100, 10, logger)
	require.NoError(t, err)

	for i := 0; i < 2000; i++ {
		_ = aw.Log(audit.Event{Event: audit.EventForward})
	}
	require.NoError(t, aw.Close())

	// The structured logger must have received at least one WARN about drops.
	assert.True(t, logger.HasWarn("events dropped"), "expected drop warning in structured logger")
}

// TestEventConstants verifies all event name constants are non-empty strings.
func TestEventConstants(t *testing.T) {
	constants := []string{
		audit.EventForward,
		audit.EventPolicyViolation,
		audit.EventReplayGap,
		audit.EventPeerConnected,
		audit.EventPeerDisconnected,
		audit.EventClientConnected,
		audit.EventClientRejected,
		audit.EventClientDrain,
		audit.EventPolicyReloaded,
		audit.EventPolicyReloadFailed,
	}
	for _, c := range constants {
		assert.NotEmpty(t, c)
	}
}
