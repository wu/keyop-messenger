//go:build integration

package federation

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestHubServerEnforcesRaisedRecvLimit stands up a real Hub and publishes over a
// raw gRPC stream (no client-side send cap) to prove the hub's receive limit is
// actually wired to grpcMessageLimit: a batch within the limit is accepted, and
// one over it is rejected with ResourceExhausted rather than silently dropped or
// hanging. Limits are shrunk so the boundary sits at a few KiB.
func TestHubServerEnforcesRaisedRecvLimit(t *testing.T) {
	const (
		recordCap = 2 * 1024
		headroom  = 1 * 1024
		maxBatch  = 2 * 1024
	)
	setTestLimits(t, recordCap, headroom)
	limit := grpcMessageLimit(maxBatch) // 3 KiB

	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := HubConfig{AllowedPeers: []AllowedPeer{{Name: "peer1", Publish: []string{"events"}}}}
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)

	var written []string
	writer := func(envs []*envelope.Envelope) error {
		for _, env := range envs {
			written = append(written, env.ID)
		}
		return nil
	}
	hub := NewHub(cfg, nil, writer, dd, &fakeAuditLogger2{}, &testutil.FakeLogger{}, 1000, maxBatch, dataDir)
	require.NoError(t, hub.Listen(":0"))
	t.Cleanup(func() { _ = hub.Close() })

	// Raw client connection — deliberately no MaxCallSendMsgSize, so the client
	// will send an oversized batch and let the hub's receive limit reject it.
	conn, err := grpc.NewClient(hub.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stub := federationv1.NewFederationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	md := metadata.Pairs("x-federation-instance", "peer1")
	stream, err := stub.Publish(metadata.NewOutgoingContext(ctx, md))
	require.NoError(t, err)

	// A record under the limit is accepted and acked.
	env, err := envelope.NewEnvelope("events", "peer1", "test.Type", map[string]any{"id": "ok"})
	require.NoError(t, err)
	rec, err := envelope.Marshal(env)
	require.NoError(t, err)
	require.Less(t, len(rec), limit, "control record must be under the frame limit")
	require.NoError(t, stream.Send(&federationv1.PublishBatch{Records: [][]byte{rec}}))

	ack, err := stream.Recv()
	require.NoError(t, err, "in-limit batch must be accepted")
	assert.Equal(t, env.ID, ack.LastId)

	// A record over the limit must be rejected by the hub's receive limit.
	oversized := make([]byte, limit+1024)
	require.NoError(t, stream.Send(&federationv1.PublishBatch{Records: [][]byte{oversized}}))

	_, err = stream.Recv()
	require.Error(t, err, "over-limit batch must fail rather than hang or be dropped")
	assert.Equal(t, codes.ResourceExhausted, status.Code(err),
		"hub must reject the oversized message with ResourceExhausted")
}
