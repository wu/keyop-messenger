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
