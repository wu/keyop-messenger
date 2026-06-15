//nolint:gosec // test file: G302 (os.Chmod to set/restore test permissions)
package federation

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// ---- helpers ----------------------------------------------------------------

// newSweepTestHub builds a minimal Hub for sweepStaleOffsets unit tests.
// fakeAuditLogger is defined in client_unit_test.go (same package).
func newSweepTestHub(t *testing.T, dataDir string) (*Hub, *testutil.FakeLogger) {
	t.Helper()
	dd, err := dedup.NewLRUDedup(100)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	h := NewHub(HubConfig{}, nil,
		func(_ *envelope.Envelope) error { return nil },
		dd, &fakeAuditLogger{}, log, 100, 65536, dataDir)
	return h, log
}

// makeFedOffsetFile writes a minimal fed-*.offset file in channelDir and
// back-dates its mtime by age (pass 0 to leave the mtime at "now").
func makeFedOffsetFile(t *testing.T, channelDir, name string, age time.Duration) string {
	t.Helper()
	path := filepath.Join(channelDir, name)
	require.NoError(t, os.WriteFile(path, []byte("0\n"), 0o600))
	if age > 0 {
		mtime := time.Now().Add(-age)
		require.NoError(t, os.Chtimes(path, mtime, mtime))
	}
	return path
}

// subsDir returns the path that sweepStaleOffsets reads.
func subsDir(dataDir string) string {
	return filepath.Join(dataDir, "subscribers")
}

// ---- tests ------------------------------------------------------------------

// TestSweepStaleOffsets_EmptyDataDir verifies that an empty dataDir is a no-op
// and does not panic.
func TestSweepStaleOffsets_EmptyDataDir(t *testing.T) {
	h, _ := newSweepTestHub(t, "")
	h.sweepStaleOffsets(time.Hour)
}

// TestSweepStaleOffsets_NoSubscribersDir verifies that a missing subscribers/
// directory is silently tolerated — no error is logged.
func TestSweepStaleOffsets_NoSubscribersDir(t *testing.T) {
	dataDir := t.TempDir()
	h, log := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)
	assert.False(t, log.HasError("TTL sweep"), "missing subscribers dir must not log an error")
}

// TestSweepStaleOffsets_DeletesStaleFile verifies that a fed-*.offset file
// whose mtime is beyond the TTL is removed and the deletion is logged at Info.
func TestSweepStaleOffsets_DeletesStaleFile(t *testing.T) {
	dataDir := t.TempDir()
	chDir := filepath.Join(subsDir(dataDir), "orders")
	require.NoError(t, os.MkdirAll(chDir, 0o750))
	path := makeFedOffsetFile(t, chDir, "fed-peer1.offset", 2*time.Hour)

	h, log := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err), "stale offset file must be deleted")
	assert.True(t, log.HasInfo("TTL sweep removed stale offset"))
}

// TestSweepStaleOffsets_KeepsFreshFile verifies that a recently-updated
// fed-*.offset file is not removed.
func TestSweepStaleOffsets_KeepsFreshFile(t *testing.T) {
	dataDir := t.TempDir()
	chDir := filepath.Join(subsDir(dataDir), "orders")
	require.NoError(t, os.MkdirAll(chDir, 0o750))
	path := makeFedOffsetFile(t, chDir, "fed-peer1.offset", 0) // mtime = now

	h, _ := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	_, err := os.Stat(path)
	assert.NoError(t, err, "fresh offset file must not be deleted")
}

// TestSweepStaleOffsets_MixedAges verifies the boundary: the stale file is
// deleted and the fresh file is kept when both coexist in the same channel.
func TestSweepStaleOffsets_MixedAges(t *testing.T) {
	dataDir := t.TempDir()
	chDir := filepath.Join(subsDir(dataDir), "orders")
	require.NoError(t, os.MkdirAll(chDir, 0o750))
	stale := makeFedOffsetFile(t, chDir, "fed-peer1.offset", 2*time.Hour)
	fresh := makeFedOffsetFile(t, chDir, "fed-peer2.offset", 0)

	h, _ := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	_, err := os.Stat(stale)
	assert.True(t, os.IsNotExist(err), "stale file must be deleted")
	_, err = os.Stat(fresh)
	assert.NoError(t, err, "fresh file must be kept")
}

// TestSweepStaleOffsets_IgnoresNonFedFiles verifies that files which do not
// match the fed-*.offset pattern are never removed, regardless of age.
func TestSweepStaleOffsets_IgnoresNonFedFiles(t *testing.T) {
	dataDir := t.TempDir()
	chDir := filepath.Join(subsDir(dataDir), "orders")
	require.NoError(t, os.MkdirAll(chDir, 0o750))

	// All back-dated past the TTL; none should be deleted.
	names := []string{
		"local.offset",  // no fed- prefix
		"sub1.offset",   // no fed- prefix
		"fed-peer1.txt", // fed- prefix but wrong suffix
		"notfed.offset", // no fed- prefix
	}
	for _, name := range names {
		makeFedOffsetFile(t, chDir, name, 2*time.Hour)
	}

	h, _ := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	for _, name := range names {
		_, err := os.Stat(filepath.Join(chDir, name))
		assert.NoError(t, err, "non-fed file %q must not be deleted", name)
	}
}

// TestSweepStaleOffsets_MultipleChannels verifies that the sweep visits every
// channel subdirectory and removes stale files in each.
func TestSweepStaleOffsets_MultipleChannels(t *testing.T) {
	dataDir := t.TempDir()
	channels := []string{"orders", "events", "metrics"}
	for _, ch := range channels {
		d := filepath.Join(subsDir(dataDir), ch)
		require.NoError(t, os.MkdirAll(d, 0o750))
		makeFedOffsetFile(t, d, "fed-peer1.offset", 2*time.Hour)
	}

	h, _ := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	for _, ch := range channels {
		_, err := os.Stat(filepath.Join(subsDir(dataDir), ch, "fed-peer1.offset"))
		assert.True(t, os.IsNotExist(err), "stale file in channel %q must be deleted", ch)
	}
}

// TestSweepStaleOffsets_NonDirInSubsDir verifies that regular files placed
// directly inside subscribers/ are skipped (not treated as channel directories).
func TestSweepStaleOffsets_NonDirInSubsDir(t *testing.T) {
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(subsDir(dataDir), 0o750))
	strayPath := filepath.Join(subsDir(dataDir), "stray.offset")
	require.NoError(t, os.WriteFile(strayPath, []byte("0\n"), 0o600))
	past := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(strayPath, past, past))

	h, _ := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	_, err := os.Stat(strayPath)
	assert.NoError(t, err, "file directly in subscribers/ must not be deleted")
}

// TestSweepStaleOffsets_RemoveFailure verifies that an os.Remove failure is
// logged at Error level and does not abort the rest of the sweep.
func TestSweepStaleOffsets_RemoveFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("test relies on DAC permission denial; root bypasses permission checks")
	}
	dataDir := t.TempDir()
	chDir := filepath.Join(subsDir(dataDir), "orders")
	require.NoError(t, os.MkdirAll(chDir, 0o750))
	makeFedOffsetFile(t, chDir, "fed-peer1.offset", 2*time.Hour)

	// Remove write permission from the channel directory so os.Remove fails.
	require.NoError(t, os.Chmod(chDir, 0o550))
	t.Cleanup(func() { _ = os.Chmod(chDir, 0o750) })

	h, log := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	assert.True(t, log.HasError("TTL sweep remove failed"))
}

// TestSweepStaleOffsets_UnreadableChannelDir verifies that a channel directory
// that cannot be listed is silently skipped while other channels are swept.
func TestSweepStaleOffsets_UnreadableChannelDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("test relies on DAC permission denial; root bypasses permission checks")
	}
	dataDir := t.TempDir()

	// good: readable channel with a stale file that must be deleted.
	goodDir := filepath.Join(subsDir(dataDir), "good-channel")
	require.NoError(t, os.MkdirAll(goodDir, 0o750))
	goodFile := makeFedOffsetFile(t, goodDir, "fed-peer1.offset", 2*time.Hour)

	// bad: unreadable channel directory; its file must be left untouched.
	badDir := filepath.Join(subsDir(dataDir), "bad-channel")
	require.NoError(t, os.MkdirAll(badDir, 0o750))
	badFile := makeFedOffsetFile(t, badDir, "fed-peer1.offset", 2*time.Hour)
	require.NoError(t, os.Chmod(badDir, 0o000))
	t.Cleanup(func() { _ = os.Chmod(badDir, 0o750) })

	h, _ := newSweepTestHub(t, dataDir)
	h.sweepStaleOffsets(time.Hour)

	_, err := os.Stat(goodFile)
	assert.True(t, os.IsNotExist(err), "stale file in readable channel must be deleted")

	// Restore permissions before stating to read the result.
	require.NoError(t, os.Chmod(badDir, 0o750))
	_, err = os.Stat(badFile)
	assert.NoError(t, err, "file in unreadable channel dir must not be deleted")
}
