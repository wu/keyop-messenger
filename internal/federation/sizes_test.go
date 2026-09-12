package federation

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
