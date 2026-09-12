package federation_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/testutil"
)

func writeOffsetFile(t *testing.T, dir, name string) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o750))
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("0"), 0o600))
	return path
}

// TestReapOrphanedOutboundOffsets verifies that fedout- files for hubs not in
// the configured set are removed, while configured fedout-, inbound fed-, and
// ordinary subscriber offsets are preserved.
func TestReapOrphanedOutboundOffsets(t *testing.T) {
	dataDir := t.TempDir()
	chanDir := storage.NewLayout(dataDir).OffsetDir("events")

	keepAddr := "hostA:7740"
	dropAddr := "hostB:7740"
	keep := writeOffsetFile(t, chanDir, "fedout-"+storage.SanitizeForFilename(keepAddr)+".offset")
	drop := writeOffsetFile(t, chanDir, "fedout-"+storage.SanitizeForFilename(dropAddr)+".offset")
	fedIn := writeOffsetFile(t, chanDir, "fed-peer1.offset")
	sub := writeOffsetFile(t, chanDir, "mysubscriber.offset")

	federation.ReapOrphanedOutboundOffsets(storage.NewLayout(dataDir), []string{keepAddr}, &testutil.FakeLogger{})

	assert.FileExists(t, keep, "configured hub's fedout- must be kept")
	assert.NoFileExists(t, drop, "removed hub's fedout- must be reaped")
	assert.FileExists(t, fedIn, "inbound fed- files must not be touched")
	assert.FileExists(t, sub, "ordinary subscriber offsets must not be touched")
}

// TestReapOrphanedOutboundOffsets_EmptyConfigReapsAll verifies that an empty
// configured set (client role disabled) orphans every fedout- file, while
// inbound fed- files remain.
func TestReapOrphanedOutboundOffsets_EmptyConfigReapsAll(t *testing.T) {
	dataDir := t.TempDir()
	chanDir := storage.NewLayout(dataDir).OffsetDir("events")
	out := writeOffsetFile(t, chanDir, "fedout-"+storage.SanitizeForFilename("hostA:7740")+".offset")
	fedIn := writeOffsetFile(t, chanDir, "fed-peer1.offset")

	federation.ReapOrphanedOutboundOffsets(storage.NewLayout(dataDir), nil, &testutil.FakeLogger{})

	assert.NoFileExists(t, out, "client disabled -> all fedout- reaped")
	assert.FileExists(t, fedIn)
}

// TestReapOrphanedOutboundOffsets_MultipleChannels verifies reaping spans every
// channel directory under subscribers/.
func TestReapOrphanedOutboundOffsets_MultipleChannels(t *testing.T) {
	dataDir := t.TempDir()
	dropAddr := "gone:7740"
	dropName := "fedout-" + storage.SanitizeForFilename(dropAddr) + ".offset"

	a := writeOffsetFile(t, storage.NewLayout(dataDir).OffsetDir("alpha"), dropName)
	b := writeOffsetFile(t, storage.NewLayout(dataDir).OffsetDir("beta"), dropName)

	federation.ReapOrphanedOutboundOffsets(storage.NewLayout(dataDir), []string{"other:7740"}, &testutil.FakeLogger{})

	assert.NoFileExists(t, a)
	assert.NoFileExists(t, b)
}

// TestReapOrphanedOutboundOffsets_MissingDir is a no-op (and must not panic)
// when no subscribers directory exists yet.
func TestReapOrphanedOutboundOffsets_MissingDir(t *testing.T) {
	federation.ReapOrphanedOutboundOffsets(storage.NewLayout(filepath.Join(t.TempDir(), "nope")), nil, &testutil.FakeLogger{})
}
