package federation

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setTestLimits shrinks the federation size limits for the duration of a test so
// the oversized/frame-limit paths can be exercised with kilobyte payloads, then
// restores the production values. Tests using it must NOT call t.Parallel(): they
// mutate package-level state, and Go only resumes parallel tests after all
// sequential tests (and their cleanups) have finished, so the production values
// are restored before any parallel test observes them.
func setTestLimits(t *testing.T, recordCap, headroom int) {
	t.Helper()
	origRecord, origHead := maxRecordBytes, grpcFramingHeadroom
	maxRecordBytes = recordCap
	grpcFramingHeadroom = headroom
	t.Cleanup(func() {
		maxRecordBytes = origRecord
		grpcFramingHeadroom = origHead
	})
}

// TestGRPCMessageLimit pins the production frame limit above gRPC's 4 MiB
// default (the value that caused the original ResourceExhausted loop) and checks
// that a batch limit larger than the per-record cap widens the frame.
func TestGRPCMessageLimit(t *testing.T) {
	const grpcDefault = 4 * 1024 * 1024

	// Production values: a record up to maxRecordBytes must fit, with framing
	// headroom, and the result must exceed gRPC's 4 MiB default.
	limit := grpcMessageLimit(grpcDefault)
	assert.Greater(t, limit, grpcDefault,
		"frame limit must exceed gRPC's 4 MiB default that caused the loop")
	assert.Equal(t, maxRecordBytes+grpcFramingHeadroom, limit,
		"with the default batch size the frame is sized for one max record")

	// When maxBatchBytes exceeds the per-record cap, the frame grows to fit a
	// full batch instead.
	bigBatch := maxRecordBytes + 5*1024*1024
	assert.Equal(t, bigBatch+grpcFramingHeadroom, grpcMessageLimit(bigBatch))
}

// TestOffsetPastNextLine covers the helper used to skip a record too large to
// scan: it must find the byte just past the next newline, and report not-found
// when the record has no terminating newline yet (still being written).
func TestOffsetPastNextLine(t *testing.T) {
	dir := t.TempDir()

	// "aaaa\nbbbb\n" — newline-terminated records.
	complete := filepath.Join(dir, "complete.jsonl")
	require.NoError(t, os.WriteFile(complete, []byte("aaaa\nbbbb\n"), 0o600))

	// Starting at segment offset 0, the first newline ends at byte 5.
	off, found, err := offsetPastNextLine(complete, 0, 0)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(5), off)

	// With a non-zero segment start the global offset is shifted accordingly.
	off, found, err = offsetPastNextLine(complete, 0, 100)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, int64(105), off)

	// A record still being written has no trailing newline → not found, so the
	// caller waits rather than skipping a partial line.
	partial := filepath.Join(dir, "partial.jsonl")
	require.NoError(t, os.WriteFile(partial, []byte("no newline yet"), 0o600))
	_, found, err = offsetPastNextLine(partial, 0, 0)
	require.NoError(t, err)
	assert.False(t, found)
}
