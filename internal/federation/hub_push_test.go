//nolint:gosec // test file: G301/G304/G306
package federation_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/storage"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// ---- helpers ----------------------------------------------------------------

// newHubWithData creates a Hub backed by dataDir and returns both the Hub and
// the audit log so tests can gate on EventClientConnected before notifying.
func newHubWithData(t *testing.T, cfg federation.HubConfig, dataDir string) (*federation.Hub, *fakeAuditLog) {
	t.Helper()
	dd, err := dedup.NewLRUDedup(10000)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}
	hub := federation.NewHub("hub", cfg, nil,
		func(*envelope.Envelope) error { return nil },
		dd, auditL, log, 1000, 65536, dataDir)
	return hub, auditL
}

// waitForConnected blocks until the hub has fully registered the peer's
// channelReaders in the notify registry (signalled by EventClientConnected).
// All tests must call this after hubPushHandshake and before NotifyChannel
// to avoid a race between the notification and reader registration.
func waitForConnected(t *testing.T, auditL *fakeAuditLog) {
	t.Helper()
	require.Eventually(t,
		func() bool { return auditL.has(audit.EventClientConnected) },
		2*time.Second, 5*time.Millisecond,
		"hub must register client connection before we can notify")
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

// hubPushHandshake connects cli as a subscribing peer.
func hubPushHandshake(t *testing.T, conn *websocket.Conn, peerName string, subscribe []string) {
	t.Helper()
	require.NoError(t, federation.SendHandshake(conn, federation.HandshakeMsg{
		InstanceName: peerName, Role: "client", Version: "1",
		Subscribe: subscribe,
	}))
	_, err := federation.ReceiveHandshake(conn)
	require.NoError(t, err)
}

// subscribingReceiver attaches a PeerReceiver on conn that captures delivered
// envelopes. It is cleaned up via t.Cleanup.
func subscribingReceiver(t *testing.T, conn *websocket.Conn) *captureWriter {
	t.Helper()
	cw := &captureWriter{}
	dd, err := dedup.NewLRUDedup(1000)
	require.NoError(t, err)
	pr := federation.NewPeerReceiver(
		conn, &sync.Mutex{}, nil, dd, cw.write,
		&fakeAuditLog{}, &testutil.FakeLogger{}, "hub", 65536,
	)
	t.Cleanup(pr.Close)
	return cw
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

// makeTestEnv creates a simple test envelope (mirrors the internal makeEnvelope helper).
func makeTestEnv(t *testing.T, channel, id string) envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "test-origin", "test.Type", map[string]any{"id": id})
	require.NoError(t, err)
	return env
}

// ---- hub-to-client push delivery tests -------------------------------------

// TestHubPushBasicDelivery verifies that messages written to a channel after a
// client subscribes are pushed to that client and acked correctly.
func TestHubPushBasicDelivery(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "sub1", Subscribe: []string{"events"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "sub1", []string{"events"})
	waitForConnected(t, auditL) // hub must register channelReaders before we notify

	cw := subscribingReceiver(t, cli)

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

// TestHubPushMultipleChannels verifies independent delivery on two subscribed channels.
func TestHubPushMultipleChannels(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "sub", Subscribe: []string{"alpha", "beta"}}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "sub", []string{"alpha", "beta"})
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	ea := makeTestEnv(t, "alpha", "a1")
	eb := makeTestEnv(t, "beta", "b1")
	writeSegment(t, filepath.Join(dataDir, "channels", "alpha"), 0, []envelope.Envelope{ea})
	writeSegment(t, filepath.Join(dataDir, "channels", "beta"), 0, []envelope.Envelope{eb})
	hub.NotifyChannel("alpha")
	hub.NotifyChannel("beta")

	require.Eventually(t, func() bool { return cw.count() == 2 },
		2*time.Second, 10*time.Millisecond, "client must receive one message per channel")

	ids := cw.ids()
	assert.Contains(t, ids, ea.ID)
	assert.Contains(t, ids, eb.ID)
}

// TestHubPushResumeFromOffset verifies that a reconnecting client receives only
// messages published after its last-acked position, not earlier history.
func TestHubPushResumeFromOffset(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "resume-peer"}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	channelDir := filepath.Join(dataDir, "channels", "orders")
	offsetDir := filepath.Join(dataDir, "subscribers", "orders")

	// Write one message and place an offset file pointing past it, simulating a
	// peer that already received this message in an earlier session.
	old := makeTestEnv(t, "orders", "old-msg")
	n := writeSegment(t, channelDir, 0, []envelope.Envelope{old})
	require.NoError(t, os.MkdirAll(offsetDir, 0o750))
	require.NoError(t, storage.WriteOffset(filepath.Join(offsetDir, "fed-resume-peer.offset"), n))

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "resume-peer", []string{"orders"})
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	// Write a new message and notify.
	newMsg := makeTestEnv(t, "orders", "new-msg")
	writeSegment(t, channelDir, n, []envelope.Envelope{newMsg})
	hub.NotifyChannel("orders")

	require.Eventually(t, func() bool { return cw.count() == 1 },
		2*time.Second, 10*time.Millisecond, "client must receive only new-msg")

	assert.Equal(t, []string{newMsg.ID}, cw.ids(), "old-msg must not be replayed")
}

// TestHubPushOffsetPersisted verifies that after delivery and ack the offset
// file is updated to point just past the delivered messages.
func TestHubPushOffsetPersisted(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "offset-peer"}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "offset-peer", []string{"metrics"})
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	channelDir := filepath.Join(dataDir, "channels", "metrics")
	e1 := makeTestEnv(t, "metrics", "m1")
	written := writeSegment(t, channelDir, 0, []envelope.Envelope{e1})
	hub.NotifyChannel("metrics")

	require.Eventually(t, func() bool { return cw.count() == 1 },
		2*time.Second, 10*time.Millisecond)

	// Wait for the channelReader to persist the offset after the ack.
	offsetPath := filepath.Join(dataDir, "subscribers", "metrics", "fed-offset-peer.offset")
	require.Eventually(t, func() bool {
		offset, err := storage.ReadOffset(offsetPath)
		return err == nil && offset == written
	}, time.Second, 10*time.Millisecond, "offset file must advance to %d", written)
}

// TestHubPushBatchSizeLimit verifies that a size-limited batch causes the
// channelReader to loop and deliver remaining messages in subsequent batches.
func TestHubPushBatchSizeLimit(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "batch-peer"}},
	}
	dd, err := dedup.NewLRUDedup(10000)
	require.NoError(t, err)
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}
	// Small maxBatchBytes forces the channelReader to split into multiple batches.
	hub := federation.NewHub("hub", cfg, nil,
		func(*envelope.Envelope) error { return nil },
		dd, auditL, log, 1000, 512, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "batch-peer", []string{"stream"})
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	channelDir := filepath.Join(dataDir, "channels", "stream")
	const total = 20
	envs := make([]envelope.Envelope, total)
	for i := range envs {
		envs[i] = makeTestEnv(t, "stream", fmt.Sprintf("msg-%02d", i))
	}
	writeSegment(t, channelDir, 0, envs)
	hub.NotifyChannel("stream")

	require.Eventually(t, func() bool { return cw.count() == total },
		3*time.Second, 20*time.Millisecond,
		"all %d messages must arrive across multiple batches", total)

	// Verify no message was delivered twice.
	seen := make(map[string]int)
	for _, id := range cw.ids() {
		seen[id]++
	}
	for _, env := range envs {
		assert.Equal(t, 1, seen[env.ID], "message %s delivered more than once", env.ID)
	}
}

// TestHubPushCorruptRecordSkipped verifies that a line that fails JSON parsing
// is silently skipped and the surrounding valid messages are still delivered.
func TestHubPushCorruptRecordSkipped(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "corrupt-peer"}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "corrupt-peer", []string{"logs"})
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	channelDir := filepath.Join(dataDir, "channels", "logs")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	good1 := makeTestEnv(t, "logs", "good-1")
	good2 := makeTestEnv(t, "logs", "good-2")
	b1, err := envelope.Marshal(good1)
	require.NoError(t, err)
	b2, err := envelope.Marshal(good2)
	require.NoError(t, err)

	// Sandwich a line of invalid JSON between two valid envelopes.
	var data []byte
	data = append(data, b1...)
	data = append(data, '\n')
	data = append(data, []byte(`{{{not valid json}}}`)...)
	data = append(data, '\n')
	data = append(data, b2...)
	data = append(data, '\n')

	require.NoError(t, os.WriteFile(
		filepath.Join(channelDir, fmt.Sprintf("%020d.jsonl", 0)),
		data, 0o600,
	))
	hub.NotifyChannel("logs")

	require.Eventually(t, func() bool { return cw.count() == 2 },
		2*time.Second, 10*time.Millisecond,
		"both valid messages must be delivered despite the corrupt middle record")

	ids := cw.ids()
	assert.Contains(t, ids, good1.ID)
	assert.Contains(t, ids, good2.ID)
}

// TestHubPushMultipleSegments verifies delivery when messages span more than
// one segment file.
func TestHubPushMultipleSegments(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "seg-peer"}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "seg-peer", []string{"items"})
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	channelDir := filepath.Join(dataDir, "channels", "items")
	e1 := makeTestEnv(t, "items", "seg1-msg")
	e2 := makeTestEnv(t, "items", "seg2-msg")
	seg1Size := writeSegment(t, channelDir, 0, []envelope.Envelope{e1})
	writeSegment(t, channelDir, seg1Size, []envelope.Envelope{e2})
	hub.NotifyChannel("items")

	require.Eventually(t, func() bool { return cw.count() == 2 },
		2*time.Second, 10*time.Millisecond,
		"messages in two segment files must both be delivered")

	ids := cw.ids()
	assert.Contains(t, ids, e1.ID)
	assert.Contains(t, ids, e2.ID)
}

// TestHubPushNotDeliveredWithoutSubscription verifies that messages on a channel
// the client did not request are not pushed to it.
func TestHubPushNotDeliveredWithoutSubscription(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	// Hub allows client to subscribe to both, but client only requests "wanted".
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "selective"}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "selective", []string{"wanted"}) // not "unwanted"
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	ew := makeTestEnv(t, "wanted", "w1")
	eu := makeTestEnv(t, "unwanted", "u1")
	writeSegment(t, filepath.Join(dataDir, "channels", "wanted"), 0, []envelope.Envelope{ew})
	writeSegment(t, filepath.Join(dataDir, "channels", "unwanted"), 0, []envelope.Envelope{eu})
	hub.NotifyChannel("wanted")
	hub.NotifyChannel("unwanted")

	require.Eventually(t, func() bool { return cw.count() >= 1 },
		2*time.Second, 10*time.Millisecond, "subscribed channel must deliver")

	time.Sleep(100 * time.Millisecond) // allow any spurious delivery to arrive

	ids := cw.ids()
	assert.Contains(t, ids, ew.ID, "subscribed channel message must arrive")
	assert.NotContains(t, ids, eu.ID, "unsubscribed channel message must not arrive")
}

// TestHubPushChannelAllowlistEnforced verifies that the hub's per-peer subscribe
// allowlist is respected: a channel not permitted by the hub is not delivered
// even if the client requests it.
func TestHubPushChannelAllowlistEnforced(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{
			Name:      "limited",
			Subscribe: []string{"allowed-ch"}, // hub restricts to "allowed-ch" only
		}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	// Client requests both channels; hub must restrict to allowed-ch.
	hubPushHandshake(t, cli, "limited", []string{"allowed-ch", "blocked-ch"})
	waitForConnected(t, auditL)

	cw := subscribingReceiver(t, cli)

	ea := makeTestEnv(t, "allowed-ch", "a1")
	eb := makeTestEnv(t, "blocked-ch", "b1")
	writeSegment(t, filepath.Join(dataDir, "channels", "allowed-ch"), 0, []envelope.Envelope{ea})
	writeSegment(t, filepath.Join(dataDir, "channels", "blocked-ch"), 0, []envelope.Envelope{eb})
	hub.NotifyChannel("allowed-ch")
	hub.NotifyChannel("blocked-ch")

	require.Eventually(t, func() bool { return cw.count() >= 1 },
		2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	ids := cw.ids()
	assert.Contains(t, ids, ea.ID, "allowed channel must be delivered")
	assert.NotContains(t, ids, eb.ID, "blocked channel must not be delivered")
}

// TestHubPushAuditEvents verifies that connecting a subscribing peer logs
// client_connected (with the subscribed channels) and peer_disconnected.
func TestHubPushAuditEvents(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	auditL := &fakeAuditLog{}
	dd, _ := dedup.NewLRUDedup(1000)
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "audit-peer", Subscribe: []string{"ch"}}},
	}
	hub := federation.NewHub("hub", cfg, nil,
		func(*envelope.Envelope) error { return nil },
		dd, auditL, &testutil.FakeLogger{}, 1000, 65536, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "audit-peer", []string{"ch"})

	require.Eventually(t, func() bool { return auditL.has(audit.EventClientConnected) },
		time.Second, 5*time.Millisecond, "client_connected must fire")

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "sub=", "Detail should list subscribed channels")
	assert.Contains(t, evt.Detail, "ch")

	// Deliver one message and confirm receipt.
	cw := subscribingReceiver(t, cli)
	writeSegment(t, filepath.Join(dataDir, "channels", "ch"), 0,
		[]envelope.Envelope{makeTestEnv(t, "ch", "audit-msg")})
	hub.NotifyChannel("ch")
	require.Eventually(t, func() bool { return cw.count() == 1 },
		2*time.Second, 10*time.Millisecond)

	// Disconnect and verify the peer_disconnected event.
	_ = cli.Close()
	require.Eventually(t, func() bool { return auditL.has(audit.EventPeerDisconnected) },
		time.Second, 5*time.Millisecond, "peer_disconnected must fire after close")
}

// TestHubPushWrongVersionRecordSkipped verifies that a valid JSON line whose
// "v" field does not match CurrentVersion is skipped; subsequent valid messages
// are still delivered.
func TestHubPushWrongVersionRecordSkipped(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "ver-peer"}},
	}
	hub, auditL := newHubWithData(t, cfg, dataDir)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	hubPushHandshake(t, cli, "ver-peer", []string{"feed"})
	waitForConnected(t, auditL)
	cw := subscribingReceiver(t, cli)

	channelDir := filepath.Join(dataDir, "channels", "feed")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	good := makeTestEnv(t, "feed", "valid-1")
	goodBytes, err := envelope.Marshal(good)
	require.NoError(t, err)

	// A JSON object missing the required "v":1 field (envelope.Unmarshal will
	// return ErrUnknownVersion and the channelReader skips it).
	badVersion := []byte(`{"v":999,"id":"bad","channel":"feed","origin":"x","ts":"2024-01-01T00:00:00Z","payload_type":"t","payload":"e30="}`)

	var data []byte
	data = append(data, badVersion...)
	data = append(data, '\n')
	data = append(data, goodBytes...)
	data = append(data, '\n')

	require.NoError(t, os.WriteFile(
		filepath.Join(channelDir, fmt.Sprintf("%020d.jsonl", 0)),
		data, 0o600,
	))
	hub.NotifyChannel("feed")

	require.Eventually(t, func() bool { return cw.count() == 1 },
		2*time.Second, 10*time.Millisecond,
		"valid envelope after wrong-version record must still be delivered")
	assert.Contains(t, cw.ids(), good.ID)
}
