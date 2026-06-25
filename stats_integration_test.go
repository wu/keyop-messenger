//go:build integration

package messenger

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStats_FederationClients verifies that Stats() surfaces connected
// federation client state when a client is dialled to a hub.
func TestStats_FederationClients(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "client-a")

	hubM := newHubMessenger(t, "hub", filepath.Join(dir, "hub"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{{Name: "client-a"}},
		},
	)
	hubAddr := hubLocalAddr(t, hubM)

	clientM := newClientMessengerWithPolicy(t, "client-a", dir, caFile,
		certFor("client-a"), keyFor("client-a"), hubAddr,
		nil, []string{"events"},
	)

	// Publish a message to confirm the connection is live.
	require.NoError(t, clientM.Publish(context.Background(), "events", "test.E", map[string]any{"v": 1}))

	// Allow connection to establish.
	require.Eventually(t, func() bool {
		s := clientM.Stats()
		return len(s.Federation.Clients) == 1 && s.Federation.Clients[0].Connected
	}, 3*time.Second, 50*time.Millisecond, "federation client did not appear as connected")

	s := clientM.Stats()
	require.Len(t, s.Federation.Clients, 1)
	cs := s.Federation.Clients[0]
	assert.Equal(t, hubAddr, cs.HubAddr)
	assert.True(t, cs.Connected)
	assert.Zero(t, cs.ReconnectCount)
}

// TestStats_FederationAckRTT verifies that once a published message is forwarded
// to the hub and acknowledged, the client's PublishRTT aggregate records the
// send→ack round-trip.
func TestStats_FederationAckRTT(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "client-a")

	hubM := newHubMessenger(t, "hub", filepath.Join(dir, "hub"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{{Name: "client-a"}},
		},
	)
	hubAddr := hubLocalAddr(t, hubM)

	clientM := newClientMessengerWithPolicy(t, "client-a", dir, caFile,
		certFor("client-a"), keyFor("client-a"), hubAddr,
		nil, []string{"events"},
	)

	require.NoError(t, clientM.Publish(context.Background(), "events", "test.E", map[string]any{"v": 1}))

	// The message is read from the local channel file, sent to the hub, and
	// acked asynchronously; poll until the RTT sample is recorded.
	var cs ClientStats
	require.Eventually(t, func() bool {
		s := clientM.Stats()
		if len(s.Federation.Clients) != 1 {
			return false
		}
		cs = s.Federation.Clients[0]
		return cs.PublishRTT.Count >= 1
	}, 3*time.Second, 50*time.Millisecond, "publish ack RTT sample was not recorded")

	assert.Positive(t, cs.PublishRTT.SumNanos, "ack RTT should have a positive duration")
}

// TestStats_HubInbound verifies that a hub instance surfaces inbound, hub-side
// metrics: an active publish stream, committed records, and accept counts.
func TestStats_HubInbound(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "client-a")

	hubM := newHubMessenger(t, "hub", filepath.Join(dir, "hub"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{{Name: "client-a"}},
		},
	)
	hubAddr := hubLocalAddr(t, hubM)

	// A hub with no connections still reports a non-nil, listening Hub snapshot.
	pre := hubM.Stats()
	require.NotNil(t, pre.Federation.Hub)
	assert.True(t, pre.Federation.Hub.Listening)
	assert.Empty(t, pre.Federation.Hub.Peers)

	clientM := newClientMessengerWithPolicy(t, "client-a", dir, caFile,
		certFor("client-a"), keyFor("client-a"), hubAddr,
		nil, []string{"events"},
	)

	require.NoError(t, clientM.Publish(context.Background(), "events", "test.E", map[string]any{"v": 1}))

	// The hub should register an inbound publish stream and commit the record.
	require.Eventually(t, func() bool {
		hub := hubM.Stats().Federation.Hub
		return hub != nil && hub.PublishConns >= 1 && hub.RecordsReceived >= 1
	}, 3*time.Second, 50*time.Millisecond, "hub did not record inbound publish")

	hub := hubM.Stats().Federation.Hub
	require.NotNil(t, hub)
	assert.True(t, hub.Listening)
	assert.Equal(t, hubM.HubAddr(), hub.Addr)
	assert.GreaterOrEqual(t, hub.ConnectionsAccepted, int64(1))
	assert.Zero(t, hub.ConnectionsRejected)
	assert.GreaterOrEqual(t, hub.BatchesReceived, int64(1))

	var found bool
	for _, p := range hub.Peers {
		if p.Peer == "client-a" && p.Kind == "publish" {
			found = true
			// The client declares its publish channels via metadata, so the hub
			// reports them even though the hub has no publish allowlist for the peer.
			assert.Equal(t, []string{"events"}, p.Channels)
			assert.False(t, p.ConnectedAt.IsZero())
		}
	}
	assert.True(t, found, "expected an inbound publish peer for client-a")
}

// TestStats_HubRejectsUnknownPeer verifies that a peer outside the allowlist
// increments the hub's ConnectionsRejected counter.
func TestStats_HubRejectsUnknownPeer(t *testing.T) {
	dir := t.TempDir()
	caFile, certFor, keyFor := integrationTLS(t, dir, "localhost", "intruder")

	hubM := newHubMessenger(t, "hub", filepath.Join(dir, "hub"), caFile,
		certFor("localhost"), keyFor("localhost"),
		HubConfig{
			AllowedPeers: []AllowedPeer{{Name: "client-a"}}, // "intruder" not allowed
		},
	)
	hubAddr := hubLocalAddr(t, hubM)

	intruder := newClientMessengerWithPolicy(t, "intruder", dir, caFile,
		certFor("intruder"), keyFor("intruder"), hubAddr,
		nil, []string{"events"},
	)
	// The publish stream will be refused by the allowlist; the error surfaces
	// asynchronously, so drive traffic and poll the reject counter.
	_ = intruder.Publish(context.Background(), "events", "test.E", map[string]any{"v": 1})

	require.Eventually(t, func() bool {
		hub := hubM.Stats().Federation.Hub
		return hub != nil && hub.ConnectionsRejected >= 1
	}, 3*time.Second, 50*time.Millisecond, "hub did not record a rejected connection")

	assert.Zero(t, hubM.Stats().Federation.Hub.PublishConns)
}
