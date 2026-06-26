package federation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// commitInboundBatch is unexported, so these size-enforcement tests live in
// package federation. They cover the hub Publish ingest path, which now passes
// the configured MaxBatchBytes instead of 0 (issue: ingest ignored the limit).

func mustRecord(t *testing.T, channel, payloadID string, padBytes int) []byte {
	t.Helper()
	payload := map[string]any{"id": payloadID}
	if padBytes > 0 {
		payload["pad"] = strings.Repeat("x", padBytes)
	}
	env, err := envelope.NewEnvelope(channel, "peer1", "test.Type", payload)
	require.NoError(t, err)
	rec, err := envelope.Marshal(env)
	require.NoError(t, err)
	return rec
}

// TestCommitInboundBatch_SkipsOversizedRecord verifies that with maxBatchBytes>0
// a record larger than the limit is skipped (not written), while a small record
// in the same batch is committed.
func TestCommitInboundBatch_SkipsOversizedRecord(t *testing.T) {
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)

	var written []string
	writer := func(envs []*envelope.Envelope) error {
		for _, env := range envs {
			written = append(written, env.ID)
		}
		return nil
	}

	small := mustRecord(t, "events", "small", 0)
	big := mustRecord(t, "events", "big", 4096)
	maxBatchBytes := (len(small) + len(big)) / 2 // between the two sizes

	smallEnv, err := envelope.Unmarshal(small)
	require.NoError(t, err)
	bigEnv, err := envelope.Unmarshal(big)
	require.NoError(t, err)

	_, err = commitInboundBatch([][]byte{small, big}, nil, dd, writer,
		&fakeAuditLogger{}, &testutil.FakeLogger{}, "peer1", maxBatchBytes)
	require.NoError(t, err)

	assert.Equal(t, []string{smallEnv.ID}, written,
		"only the in-limit record should be written; oversized one skipped")
	assert.NotContains(t, written, bigEnv.ID)
}

// TestCommitInboundBatch_ZeroDisablesSizeCheck verifies the previous behavior is
// still reachable: maxBatchBytes=0 writes records regardless of size.
func TestCommitInboundBatch_ZeroDisablesSizeCheck(t *testing.T) {
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)

	var count int
	writer := func(envs []*envelope.Envelope) error {
		count += len(envs)
		return nil
	}

	big := mustRecord(t, "events", "big", 8192)
	_, err = commitInboundBatch([][]byte{big}, nil, dd, writer,
		&fakeAuditLogger{}, &testutil.FakeLogger{}, "peer1", 0)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "maxBatchBytes=0 must not skip by size")
}
