//nolint:gosec // test file: G306
package storage

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteReadOffset_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.offset")

	for _, offset := range []int64{0, 1, math.MaxInt64 / 2, math.MaxInt64} {
		require.NoError(t, WriteOffset(path, offset))
		got, err := ReadOffset(path)
		require.NoError(t, err)
		assert.Equal(t, offset, got, "offset %d round-trip failed", offset)
	}
}

func TestWriteOffset_NoTmpFileRemains(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.offset")

	require.NoError(t, WriteOffset(path, 42))

	_, err := os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(err), ".tmp file must not remain after successful WriteOffset")
}

func TestWriteOffset_Atomic_OriginalPreserved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.offset")

	// Write an initial valid offset.
	require.NoError(t, WriteOffset(path, 100))

	// Place a corrupt .tmp file (simulating a crash between write and rename).
	// ReadOffset on the real file must still return the original value.
	require.NoError(t, os.WriteFile(path+".tmp", []byte("corrupt"), 0o644))

	got, err := ReadOffset(path)
	require.NoError(t, err)
	assert.Equal(t, int64(100), got,
		"original offset file must be unaffected by a stale .tmp file")
}

func TestReadOffset_Missing(t *testing.T) {
	dir := t.TempDir()
	_, err := ReadOffset(filepath.Join(dir, "missing.offset"))
	require.Error(t, err)
}

func TestReadOffset_Corrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.offset")
	require.NoError(t, os.WriteFile(path, []byte("not-a-number\n"), 0o644))
	_, err := ReadOffset(path)
	require.Error(t, err)
}

func TestOffsetFileExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.offset")

	assert.False(t, OffsetFileExists(path), "should not exist before write")
	require.NoError(t, WriteOffset(path, 0))
	assert.True(t, OffsetFileExists(path), "should exist after write")
}

// TestWriteOffset_CreateFailure verifies the error path when the temporary file
// cannot be created (e.g. the parent directory does not exist).
func TestWriteOffset_CreateFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nonexistent", "sub.offset")

	err := WriteOffset(path, 42)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create tmp offset file")
}

// TestWriteOffset_RenameFailure verifies the error path when the atomic rename
// fails (target path is a directory) and that the .tmp file is cleaned up.
func TestWriteOffset_RenameFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub.offset")
	// Create a directory at the target path so os.Rename(tmp, path) fails with EISDIR.
	require.NoError(t, os.MkdirAll(path, 0o750))

	err := WriteOffset(path, 42)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rename offset file")

	// The .tmp file must be cleaned up after the failed rename.
	_, statErr := os.Stat(path + ".tmp")
	assert.True(t, os.IsNotExist(statErr), ".tmp file must not remain after rename failure")
}
