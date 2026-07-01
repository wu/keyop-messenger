package federation

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// recordingAuditLogger captures logged events for assertions.
type recordingAuditLogger struct{ events []audit.Event }

func (r *recordingAuditLogger) Log(e audit.Event) error { r.events = append(r.events, e); return nil }
func (r *recordingAuditLogger) Close() error            { return nil }

func (r *recordingAuditLogger) count(name string) int {
	n := 0
	for _, e := range r.events {
		if e.Event == name {
			n++
		}
	}
	return n
}

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
		&fakeAuditLogger{}, &testutil.FakeLogger{}, "peer1", "self", maxBatchBytes)
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
		&fakeAuditLogger{}, &testutil.FakeLogger{}, "peer1", "self", 0)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "maxBatchBytes=0 must not skip by size")
}

// loopedRecord builds a marshaled envelope whose Route already contains
// visitedBy, simulating a message that has looped back to a node that holds a
// durable copy of it.
func loopedRecord(t *testing.T, channel, payloadID string, visitedBy ...string) []byte {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "origin-host", "test.Type", map[string]any{"id": payloadID})
	require.NoError(t, err)
	for _, v := range visitedBy {
		env.AppendRoute(v)
	}
	rec, err := envelope.Marshal(env)
	require.NoError(t, err)
	return rec
}

// TestCommitInboundBatch_DropsLoopedMessage verifies the path-vector loop guard:
// a record whose Route already names this node is dropped (not committed) even
// when the dedup cache has no memory of it, while a record that has not visited
// this node is committed with the node appended to its Route.
func TestCommitInboundBatch_DropsLoopedMessage(t *testing.T) {
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)

	var written []*envelope.Envelope
	writer := func(envs []*envelope.Envelope) error {
		written = append(written, envs...)
		return nil
	}

	// echo: this node ("hub-a") is already in the route → must be dropped.
	echo := loopedRecord(t, "metrics", "echo", "hub-a")
	// fresh: has visited only origin-host and hub-b → must be committed.
	fresh := loopedRecord(t, "metrics", "fresh", "hub-b")

	audits := &recordingAuditLogger{}
	_, err = commitInboundBatch([][]byte{echo, fresh}, nil, dd, writer,
		audits, &testutil.FakeLogger{}, "peer-hub-b", "hub-a", 0)
	require.NoError(t, err)

	require.Len(t, written, 1, "only the non-looping record should be committed")
	assert.Contains(t, string(written[0].Payload), `"fresh"`,
		"the committed record must be the one that had not visited this node")
	assert.True(t, written[0].RouteContains("hub-a"),
		"this node must append itself to the committed record's Route")
	assert.Equal(t, 1, audits.count(audit.EventLoopDropped),
		"the dropped echo must be audited as a loop")
}
