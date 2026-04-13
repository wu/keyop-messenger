//go:build integration
// +build integration

//nolint:gosec // test file
package messenger

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMessenger_Client_PublishAllowlist verifies that when a client has a
// restricted publish allowlist, it does NOT forward messages on disallowed channels.
//
// Setup:
// - Hub accepts a client named "restricted-client" with publish: ["movie"]
// - Client publishes to both "movie" and "movies" channels
//
// Expected:
// - Message on "movie" reaches the hub
// - Message on "movies" does NOT reach the hub (blocked by client's allowlist)
//
// BUG (before fix): Both messages reach the hub because the client sends all channels
func TestMessenger_Client_PublishAllowlist(t *testing.T) {
	tmpDir := t.TempDir()

	// Generate TLS certs - use "localhost" as the hub cert name for proper TLS verification
	caFile, certFor, keyFor := integrationTLS(t, tmpDir, "localhost", "restricted-client")

	// Create hub with NO publish restrictions on the client
	// (hub should accept any channel the client sends)
	hubCfg := HubConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
		AllowedPeers: []AllowedPeer{
			{
				Name:    "restricted-client",
				Publish: []string{}, // Empty means hub accepts any channel from this peer
			},
		},
	}

	hubMsg := newHubMessenger(t, "hub", filepath.Join(tmpDir, "hub"), caFile, certFor("localhost"), keyFor("localhost"), hubCfg)
	hubAddr := hubLocalAddr(t, hubMsg)

	// Create client with restricted publish allowlist
	clientCfg := &Config{
		Name: "restricted-client",
		Storage: StorageConfig{
			DataDir:    filepath.Join(tmpDir, "restricted-client"),
			SyncPolicy: SyncPolicyNone,
		},
		Client: ClientConfig{
			Enabled: true,
			Hubs: []ClientHubRef{
				{
					Addr:    hubAddr,
					Publish: []string{"movie"}, // Client can only publish to "movie"
				},
			},
		},
		TLS: TLSConfig{Cert: certFor("restricted-client"), Key: keyFor("restricted-client"), CA: caFile},
	}
	clientCfg.ApplyDefaults()
	clientMsg, err := New(clientCfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientMsg.Close() })

	// Give client time to connect
	time.Sleep(500 * time.Millisecond)

	ctx := context.Background()

	// Track what the hub receives on each channel
	receivedMu := &sync.Mutex{}
	receivedChannels := map[string]int{}

	// Subscribe on hub to track messages
	for _, ch := range []string{"movie", "movies"} {
		ch := ch // capture for closure
		go func() {
			err := hubMsg.Subscribe(ctx, ch, "tracker-"+ch, func(ctx context.Context, msg Message) error {
				receivedMu.Lock()
				receivedChannels[ch]++
				receivedMu.Unlock()
				return nil
			})
			if err != nil {
				t.Logf("failed to subscribe to %q: %v", ch, err)
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)

	// Test 1: Client publishes to ALLOWED channel "movie"
	t.Log("Publishing to allowed channel 'movie'")
	err = clientMsg.Publish(ctx, "movie", "test.v1", map[string]any{"msg": "allowed"})
	require.NoError(t, err)
	time.Sleep(400 * time.Millisecond)

	// Test 2: Client publishes to DISALLOWED channel "movies"
	t.Log("Publishing to disallowed channel 'movies'")
	err = clientMsg.Publish(ctx, "movies", "test.v1", map[string]any{"msg": "should-be-blocked"})
	require.NoError(t, err)
	time.Sleep(400 * time.Millisecond)

	receivedMu.Lock()
	defer receivedMu.Unlock()

	// Verify results
	t.Logf("Hub received: movie=%d, movies=%d", receivedChannels["movie"], receivedChannels["movies"])

	assert.Greater(t, receivedChannels["movie"], 0, "hub should receive message on allowed 'movie' channel")

	if receivedChannels["movies"] > 0 {
		t.Errorf("BUG CONFIRMED: Hub received %d message(s) on 'movies' channel "+
			"even though client's publish allowlist only includes ['movie']. "+
			"The client should NOT have forwarded this message.",
			receivedChannels["movies"])
	} else {
		t.Log("PASS: Hub did not receive any messages on disallowed 'movies' channel")
	}
}
