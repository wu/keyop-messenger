//go:build integration

//nolint:gosec // test file: G301/G304/G306
package federation_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	federationv1 "github.com/wu/keyop-messenger/gen/federation/v1"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// ---- helpers ----------------------------------------------------------------

func newHubWithData(t *testing.T, cfg federation.HubConfig, dataDir string) (*federation.Hub, *fakeAuditLog) {
	t.Helper()
	dd, err := dedup.NewLRUDedup(10000)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}
	hub := federation.NewHub(cfg, nil,
		func(*envelope.Envelope) error { return nil },
		dd, auditL, log, 1000, 65536, dataDir)
	return hub, auditL
}

// startHubWithData starts hub listening on ":0" and returns a stub + cleanup.
func startHubWithData(t *testing.T, hub *federation.Hub) federationv1.FederationServiceClient {
	t.Helper()
	require.NoError(t, hub.Listen(":0"))
	conn, err := grpc.NewClient(hub.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); _ = hub.Close() })
	return federationv1.NewFederationServiceClient(conn)
}

// waitForConnected blocks until the hub has registered the peer's channelReaders.
func waitForConnected(t *testing.T, auditL *fakeAuditLog) {
	t.Helper()
	require.Eventually(t,
		func() bool { return auditL.has(audit.EventClientConnected) },
		2*time.Second, 5*time.Millisecond,
		"hub must register client connection before we can notify")
}

// openSubscribeReceiver opens a Subscribe stream, sends the SubscribeRequest,
// attaches a PeerReceiver that captures delivered envelopes, and returns the
// captureWriter. Both the stream and receiver are cleaned up via t.Cleanup.
func openSubscribeReceiver(t *testing.T, stub federationv1.FederationServiceClient, peerName string, subscribe []string) *captureWriter {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	md := metadata.Pairs("x-federation-instance", peerName)
	stream, err := stub.Subscribe(metadata.NewOutgoingContext(ctx, md))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&federationv1.SubscribeFrame{
		Payload: &federationv1.SubscribeFrame_Request{
			Request: &federationv1.SubscribeRequest{
				Version:   "1",
				Subscribe: subscribe,
			},
		},
	}))

	cw := &captureWriter{}
	dd, _ := dedup.NewLRUDedup(1000)
	pr := federation.NewPeerReceiver(stream, cancel, nil, dd, cw.write, nil,
		&fakeAuditLog{}, &testutil.FakeLogger{}, peerName, 65536)
	t.Cleanup(func() { cancel(); pr.Close() })
	return cw
}

// writeSegment writes envelopes as JSONL into a segment file starting at
// startOffset and returns the number of bytes written.
func writeSegment(t *testing.T, channelDir string, startOffset int64, envs []envelope.Envelope) int64 {
	t.Helper()
	require.NoError(t, os.MkdirAll(channelDir, 0o750))
	path := filepath.Join(channelDir, fmt.Sprintf("%020d.jsonl", startOffset))
	var data []byte
	for _, env := range envs {
		b, err := envelope.Marshal(env)
		require.NoError(t, err)
		data = append(data, b...)
		data = append(data, '\n')
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return int64(len(data))
}

// captureWriter records every envelope delivered to it.
type captureWriter struct {
	mu   sync.Mutex
	envs []*envelope.Envelope
}

func (c *captureWriter) write(env *envelope.Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *env
	c.envs = append(c.envs, &cp)
	return nil
}

func (c *captureWriter) ids() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.envs))
	for i, e := range c.envs {
		out[i] = e.ID
	}
	return out
}

func (c *captureWriter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.envs)
}

// ---- hub-to-client push delivery tests -------------------------------------

func TestHubPushBasicDelivery(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "sub1", Subscribe: []string{"events"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "sub1", []string{"events"})
	waitForConnected(t, auditL)

	channelDir := filepath.Join(dataDir, "channels", "events")
	e1 := makeTestEnv(t, "events", "e1")
	e2 := makeTestEnv(t, "events", "e2")
	writeSegment(t, channelDir, 0, []envelope.Envelope{e1, e2})
	hub.NotifyChannel("events")

	require.Eventually(t, func() bool { return cw.count() == 2 },
		2*time.Second, 10*time.Millisecond, "client must receive both messages")

	ids := cw.ids()
	assert.Contains(t, ids, e1.ID)
	assert.Contains(t, ids, e2.ID)
}

func TestHubPushMultipleChannels(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "sub", Subscribe: []string{"alpha", "beta"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "sub", []string{"alpha", "beta"})
	waitForConnected(t, auditL)

	ea := makeTestEnv(t, "alpha", "a1")
	eb := makeTestEnv(t, "beta", "b1")
	writeSegment(t, filepath.Join(dataDir, "channels", "alpha"), 0, []envelope.Envelope{ea})
	writeSegment(t, filepath.Join(dataDir, "channels", "beta"), 0, []envelope.Envelope{eb})
	hub.NotifyChannel("alpha")
	hub.NotifyChannel("beta")

	require.Eventually(t, func() bool { return cw.count() == 2 },
		2*time.Second, 10*time.Millisecond)

	ids := cw.ids()
	assert.Contains(t, ids, ea.ID)
	assert.Contains(t, ids, eb.ID)
}

func TestHubPushResumeFromOffset(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "resume-peer", Subscribe: []string{"orders"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	channelDir := filepath.Join(dataDir, "channels", "orders")
	offsetDir := filepath.Join(dataDir, "subscribers", "orders")

	old := makeTestEnv(t, "orders", "old-msg")
	n := writeSegment(t, channelDir, 0, []envelope.Envelope{old})
	require.NoError(t, os.MkdirAll(offsetDir, 0o750))
	require.NoError(t, storage.WriteOffset(filepath.Join(offsetDir, "fed-resume-peer.offset"), n))

	stub := startHubWithData(t, hub)
	cw := openSubscribeReceiver(t, stub, "resume-peer", []string{"orders"})
	waitForConnected(t, auditL)

	newMsg := makeTestEnv(t, "orders", "new-msg")
	writeSegment(t, channelDir, n, []envelope.Envelope{newMsg})
	hub.NotifyChannel("orders")

	require.Eventually(t, func() bool { return cw.count() == 1 },
		2*time.Second, 10*time.Millisecond, "client must receive only new-msg")

	assert.Equal(t, []string{newMsg.ID}, cw.ids(), "old-msg must not be replayed")
}

func TestHubPushOffsetPersisted(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "offset-peer", Subscribe: []string{"metrics"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "offset-peer", []string{"metrics"})
	waitForConnected(t, auditL)

	channelDir := filepath.Join(dataDir, "channels", "metrics")
	e1 := makeTestEnv(t, "metrics", "m1")
	written := writeSegment(t, channelDir, 0, []envelope.Envelope{e1})
	hub.NotifyChannel("metrics")

	require.Eventually(t, func() bool { return cw.count() == 1 }, 2*time.Second, 10*time.Millisecond)

	offsetPath := filepath.Join(dataDir, "subscribers", "metrics", "fed-offset-peer.offset")
	require.Eventually(t, func() bool {
		offset, err := storage.ReadOffset(offsetPath)
		return err == nil && offset == written
	}, time.Second, 10*time.Millisecond, "offset file must advance to %d", written)
}

func TestHubPushBatchSizeLimit(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "batch-peer", Subscribe: []string{"stream"}}},
	}
	dd, err := dedup.NewLRUDedup(10000)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}
	hub := federation.NewHub(cfg, nil,
		func(*envelope.Envelope) error { return nil },
		dd, auditL, log, 1000, 512, dataDir)

	stub := startHubWithData(t, hub)
	cw := openSubscribeReceiver(t, stub, "batch-peer", []string{"stream"})
	waitForConnected(t, auditL)

	channelDir := filepath.Join(dataDir, "channels", "stream")
	const total = 20
	envs := make([]envelope.Envelope, total)
	for i := range envs {
		envs[i] = makeTestEnv(t, "stream", fmt.Sprintf("msg-%02d", i))
	}
	writeSegment(t, channelDir, 0, envs)
	hub.NotifyChannel("stream")

	require.Eventually(t, func() bool { return cw.count() == total },
		3*time.Second, 20*time.Millisecond, "all %d messages must arrive", total)

	seen := make(map[string]int)
	for _, id := range cw.ids() {
		seen[id]++
	}
	for _, env := range envs {
		assert.Equal(t, 1, seen[env.ID], "message %s delivered more than once", env.ID)
	}
}

func TestHubPushCorruptRecordSkipped(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "corrupt-peer", Subscribe: []string{"logs"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "corrupt-peer", []string{"logs"})
	waitForConnected(t, auditL)

	channelDir := filepath.Join(dataDir, "channels", "logs")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	good1 := makeTestEnv(t, "logs", "good-1")
	good2 := makeTestEnv(t, "logs", "good-2")
	b1, _ := envelope.Marshal(good1)
	b2, _ := envelope.Marshal(good2)

	var data []byte
	data = append(data, b1...)
	data = append(data, '\n')
	data = append(data, []byte(`{{{not valid json}}}`)...)
	data = append(data, '\n')
	data = append(data, b2...)
	data = append(data, '\n')

	require.NoError(t, os.WriteFile(
		filepath.Join(channelDir, fmt.Sprintf("%020d.jsonl", 0)), data, 0o600))
	hub.NotifyChannel("logs")

	require.Eventually(t, func() bool { return cw.count() == 2 },
		2*time.Second, 10*time.Millisecond)

	ids := cw.ids()
	assert.Contains(t, ids, good1.ID)
	assert.Contains(t, ids, good2.ID)
}

func TestHubPushMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "seg-peer", Subscribe: []string{"items"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "seg-peer", []string{"items"})
	waitForConnected(t, auditL)

	channelDir := filepath.Join(dataDir, "channels", "items")
	e1 := makeTestEnv(t, "items", "seg1-msg")
	e2 := makeTestEnv(t, "items", "seg2-msg")
	seg1Size := writeSegment(t, channelDir, 0, []envelope.Envelope{e1})
	writeSegment(t, channelDir, seg1Size, []envelope.Envelope{e2})
	hub.NotifyChannel("items")

	require.Eventually(t, func() bool { return cw.count() == 2 },
		2*time.Second, 10*time.Millisecond)

	ids := cw.ids()
	assert.Contains(t, ids, e1.ID)
	assert.Contains(t, ids, e2.ID)
}

func TestHubPushNotDeliveredWithoutSubscription(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "selective", Subscribe: []string{"wanted", "unwanted"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "selective", []string{"wanted"}) // not "unwanted"
	waitForConnected(t, auditL)

	ew := makeTestEnv(t, "wanted", "w1")
	eu := makeTestEnv(t, "unwanted", "u1")
	writeSegment(t, filepath.Join(dataDir, "channels", "wanted"), 0, []envelope.Envelope{ew})
	writeSegment(t, filepath.Join(dataDir, "channels", "unwanted"), 0, []envelope.Envelope{eu})
	hub.NotifyChannel("wanted")
	hub.NotifyChannel("unwanted")

	require.Eventually(t, func() bool { return cw.count() >= 1 },
		2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	ids := cw.ids()
	assert.Contains(t, ids, ew.ID)
	assert.NotContains(t, ids, eu.ID)
}

func TestHubPushChannelAllowlistEnforced(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{
			Name:      "limited",
			Subscribe: []string{"allowed-ch"},
		}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "limited", []string{"allowed-ch", "blocked-ch"})
	waitForConnected(t, auditL)

	ea := makeTestEnv(t, "allowed-ch", "a1")
	eb := makeTestEnv(t, "blocked-ch", "b1")
	writeSegment(t, filepath.Join(dataDir, "channels", "allowed-ch"), 0, []envelope.Envelope{ea})
	writeSegment(t, filepath.Join(dataDir, "channels", "blocked-ch"), 0, []envelope.Envelope{eb})
	hub.NotifyChannel("allowed-ch")
	hub.NotifyChannel("blocked-ch")

	require.Eventually(t, func() bool { return cw.count() >= 1 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	ids := cw.ids()
	assert.Contains(t, ids, ea.ID)
	assert.NotContains(t, ids, eb.ID)
}

func TestHubPushAuditEvents(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	auditL := &fakeAuditLog{}
	dd, _ := dedup.NewLRUDedup(1000)
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "audit-peer", Subscribe: []string{"ch"}}},
	}
	hub := federation.NewHub(cfg, nil,
		func(*envelope.Envelope) error { return nil },
		dd, auditL, &testutil.FakeLogger{}, 1000, 65536, dataDir)

	stub := startHubWithData(t, hub)

	// Open Subscribe stream manually so we can close it mid-test.
	subCtx, subCancel := context.WithCancel(context.Background())
	md := metadata.Pairs("x-federation-instance", "audit-peer")
	subStream, err := stub.Subscribe(metadata.NewOutgoingContext(subCtx, md))
	require.NoError(t, err)
	require.NoError(t, subStream.Send(&federationv1.SubscribeFrame{
		Payload: &federationv1.SubscribeFrame_Request{
			Request: &federationv1.SubscribeRequest{
				Version:   "1",
				Subscribe: []string{"ch"},
			},
		},
	}))

	cw := &captureWriter{}
	subDD, _ := dedup.NewLRUDedup(1000)
	pr := federation.NewPeerReceiver(subStream, subCancel, nil, subDD, cw.write, nil,
		&fakeAuditLog{}, &testutil.FakeLogger{}, "audit-peer", 65536)
	t.Cleanup(func() { subCancel(); pr.Close() })

	waitForConnected(t, auditL)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "sub=")
	assert.Contains(t, evt.Detail, "ch")

	writeSegment(t, filepath.Join(dataDir, "channels", "ch"), 0,
		[]envelope.Envelope{makeTestEnv(t, "ch", "audit-msg")})
	hub.NotifyChannel("ch")
	require.Eventually(t, func() bool { return cw.count() == 1 }, 2*time.Second, 10*time.Millisecond)

	// Explicitly close the subscribe stream and verify peer_disconnected fires.
	subCancel()
	require.Eventually(t, func() bool { return auditL.has(audit.EventPeerDisconnected) },
		2*time.Second, 5*time.Millisecond, "peer_disconnected must fire after close")
}

func TestHubPushWrongVersionRecordSkipped(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "ver-peer", Subscribe: []string{"feed"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)
	stub := startHubWithData(t, hub)

	cw := openSubscribeReceiver(t, stub, "ver-peer", []string{"feed"})
	waitForConnected(t, auditL)

	channelDir := filepath.Join(dataDir, "channels", "feed")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	good := makeTestEnv(t, "feed", "valid-1")
	goodBytes, _ := envelope.Marshal(good)

	badVersion := []byte(`{"v":999,"id":"bad","channel":"feed","origin":"x","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":"e30="}`)

	var data []byte
	data = append(data, badVersion...)
	data = append(data, '\n')
	data = append(data, goodBytes...)
	data = append(data, '\n')

	require.NoError(t, os.WriteFile(
		filepath.Join(channelDir, fmt.Sprintf("%020d.jsonl", 0)), data, 0o600))
	hub.NotifyChannel("feed")

	require.Eventually(t, func() bool { return cw.count() == 1 },
		2*time.Second, 10*time.Millisecond,
		"valid envelope after wrong-version record must still be delivered")
	assert.Contains(t, cw.ids(), good.ID)
}
