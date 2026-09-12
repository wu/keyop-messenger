package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLayout_Paths(t *testing.T) {
	t.Parallel()
	l := NewLayout("/data")

	assert.Equal(t, "/data", l.DataDir())
	assert.Equal(t, filepath.Join("/data", "channels", "metrics"), l.ChannelDir("metrics"))
	assert.Equal(t, filepath.Join("/data", "subscribers", "metrics"), l.OffsetDir("metrics"))
	assert.Equal(t, filepath.Join("/data", "subscribers", "metrics", "webui.offset"),
		l.OffsetPath("metrics", "webui"))
	assert.Equal(t, filepath.Join("/data", "subscribers", "metrics", "fed-hub-a.offset"),
		l.OffsetPath("metrics", OffsetPrefixFedIn+"hub-a"))
}

// TestLayout_OffsetPathSanitizes covers the reason offset IDs go through the
// layout at all: they originate in peer certificates and configured hub
// addresses, so a separator in one must not escape the channel's directory.
func TestLayout_OffsetPathSanitizes(t *testing.T) {
	t.Parallel()
	l := NewLayout("/data")

	got := l.OffsetPath("metrics", OffsetPrefixFedIn+"../../evil")
	assert.Equal(t, filepath.Join("/data", "subscribers", "metrics"), filepath.Dir(got),
		"a sanitized offset must stay inside its channel directory")
	assert.Equal(t, l.OffsetPath("metrics", "a_b"), l.OffsetPath("metrics", "a/b"),
		"sanitization maps separators to the same file")
}

func TestLayout_ChannelsAndOffsetFiles(t *testing.T) {
	t.Parallel()
	l := NewLayout(t.TempDir())

	// Nothing on disk yet: empty, not an error.
	channels, err := l.Channels()
	require.NoError(t, err)
	assert.Empty(t, channels)
	files, err := l.OffsetFiles("metrics")
	require.NoError(t, err)
	assert.Empty(t, files)

	require.NoError(t, os.MkdirAll(l.OffsetDir("metrics"), 0o750))
	require.NoError(t, os.MkdirAll(l.OffsetDir("events"), 0o750))
	require.NoError(t, WriteOffset(l.OffsetPath("metrics", "webui"), 10))
	require.NoError(t, WriteOffset(l.OffsetPath("metrics", OffsetPrefixFedIn+"hub-a"), 20))
	require.NoError(t, WriteOffset(l.OffsetPath("metrics", OffsetPrefixFedOut+"hub-b"), 30))
	// An in-flight write must never be listed as a committed offset.
	require.NoError(t, os.WriteFile(
		filepath.Join(l.OffsetDir("metrics"), "webui.offset.tmp"), []byte("99"), 0o600))

	channels, err = l.Channels()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"metrics", "events"}, channels)

	files, err = l.OffsetFiles("metrics")
	require.NoError(t, err)
	ids := make([]string, 0, len(files))
	for _, f := range files {
		ids = append(ids, f.ID)
		assert.False(t, f.ModTime.IsZero())
		assert.Equal(t, l.OffsetPath("metrics", f.ID), f.Path)
	}
	assert.ElementsMatch(t, []string{"webui", "fed-hub-a", "fedout-hub-b"}, ids,
		"every offset kind is listed, and the .tmp file is not")
}

func TestOffsetFile_PrefixHelpers(t *testing.T) {
	t.Parallel()
	f := OffsetFile{ID: OffsetPrefixFedIn + "hub-a"}

	assert.True(t, f.HasPrefix(OffsetPrefixFedIn))
	assert.False(t, f.HasPrefix(OffsetPrefixFedOut))
	assert.True(t, f.HasPrefix(""), "the empty prefix matches every kind")
	assert.Equal(t, "hub-a", f.TrimPrefix(OffsetPrefixFedIn))
}

func TestLayout_RemoveOffset(t *testing.T) {
	t.Parallel()
	l := NewLayout(t.TempDir())
	require.NoError(t, os.MkdirAll(l.OffsetDir("metrics"), 0o750))
	require.NoError(t, WriteOffset(l.OffsetPath("metrics", "webui"), 10))

	require.NoError(t, l.RemoveOffset("metrics", "webui"))
	assert.False(t, OffsetFileExists(l.OffsetPath("metrics", "webui")))

	assert.NoError(t, l.RemoveOffset("metrics", "webui"), "removing an absent offset is not an error")
	assert.NoError(t, l.RemoveOffset("no-such-channel", "webui"))
}

// TestLayout_RemoveOffsetFileDeletesWhatWasListed covers the case that makes
// RemoveOffset wrong for a listed file. Offset filenames were not always
// sanitized, so a data directory can still hold a legacy name alongside the
// sanitized one for the same reader. Re-deriving the path from the legacy ID
// resolves to the *sanitized* file — the live one — so a sweep that found the
// stale file would delete the healthy reader's offset instead.
func TestLayout_RemoveOffsetFileDeletesWhatWasListed(t *testing.T) {
	t.Parallel()
	l := NewLayout(t.TempDir())
	require.NoError(t, os.MkdirAll(l.OffsetDir("metrics"), 0o750))

	legacy := filepath.Join(l.OffsetDir("metrics"), OffsetPrefixFedOut+"hostA:7740.offset")
	live := l.OffsetPath("metrics", OffsetPrefixFedOut+"hostA:7740") // ...hostA_7740.offset
	require.NotEqual(t, legacy, live, "the legacy name must not already be sanitized")
	require.NoError(t, WriteOffset(legacy, 1))
	require.NoError(t, WriteOffset(live, 2))

	var found OffsetFile
	files, err := l.OffsetFiles("metrics")
	require.NoError(t, err)
	for _, f := range files {
		if f.Path == legacy {
			found = f
		}
	}
	require.Equal(t, legacy, found.Path, "the legacy file must be listed")

	require.NoError(t, l.RemoveOffsetFile(found))
	assert.False(t, OffsetFileExists(legacy), "the listed file is deleted")
	assert.True(t, OffsetFileExists(live), "the live offset must survive")
}

// TestLayout_RemoveOffsetFileAbsent verifies deleting an already-gone file is
// not an error — two sweeps can race over the same offset.
func TestLayout_RemoveOffsetFileAbsent(t *testing.T) {
	t.Parallel()
	l := NewLayout(t.TempDir())
	assert.NoError(t, l.RemoveOffsetFile(OffsetFile{Path: l.OffsetPath("metrics", "gone")}))
}

// TestLayout_OffsetFilesKeepsUnstattableEntry pins that a file whose stat fails
// is still listed. The compactor derives its minimum offset from this list, so a
// silently missing reader is a reader whose unconsumed segments become eligible
// for deletion.
func TestLayout_OffsetFilesKeepsUnstattableEntry(t *testing.T) {
	t.Parallel()
	l := NewLayout(t.TempDir())
	require.NoError(t, os.MkdirAll(l.OffsetDir("metrics"), 0o750))
	require.NoError(t, WriteOffset(l.OffsetPath("metrics", "webui"), 10))

	files, err := l.OffsetFiles("metrics")
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.False(t, files[0].ModTime.IsZero(), "a stattable file reports its mtime")

	// A zero ModTime is the contract for "age unknown"; age-based callers must
	// leave such a file alone rather than treat it as infinitely old.
	assert.True(t, OffsetFile{}.ModTime.IsZero())
}

func TestLayout_OffsetFilesSkipsDirectories(t *testing.T) {
	t.Parallel()
	l := NewLayout(t.TempDir())
	require.NoError(t, os.MkdirAll(filepath.Join(l.OffsetDir("metrics"), "weird.offset"), 0o750))

	files, err := l.OffsetFiles("metrics")
	require.NoError(t, err)
	assert.Empty(t, files)
}
