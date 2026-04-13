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

// TestMessenger_Client_ReceiveAllowlist verifies that when a client has a
// restricted subscribe allowlist, it DOES receive messages on those channels.
//
// Setup:
// - Hub sends messages on both "alerts" and "metrics" channels
// - Client has subscribe: ["alerts", "metrics"] (wants to receive these)
// - Client has publish: ["events", "logs"] (has nothing to do with receiving)
//
// Expected:
// - Messages on "alerts" reach the client
// - Messages on "metrics" reach the client
//
// BUG (before fix): Messages are rejected by client's receive policy because
// the policy was incorrectly set to the Publish channels instead of Subscribe channels.
func TestMessenger_Client_ReceiveAllowlist(t *testing.T) {
	tmpDir := t.TempDir()

	// Generate TLS certs
	caFile, certFor, keyFor := integrationTLS(t, tmpDir, "localhost", "restricted-client")

	// Create hub with a client that can subscribe to specific channels
	hubCfg := HubConfig{
		Enabled:    true,
		ListenAddr: "127.0.0.1:0",
		AllowedPeers: []AllowedPeer{
			{
				Name:      "restricted-client",
				Subscribe: []string{"alerts", "metrics"}, // Hub will send these to the client
			},
		},
	}

	hubMsg := newHubMessenger(t, "hub", filepath.Join(tmpDir, "hub"), caFile, certFor("localhost"), keyFor("localhost"), hubCfg)
	hubAddr := hubLocalAddr(t, hubMsg)

	// Create client that subscribes to specific channels (but also publishes on different ones)
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
					Addr:      hubAddr,
					Subscribe: []string{"alerts", "metrics"}, // Client wants to receive these
					Publish:   []string{"events", "logs"},    // Client publishes these (should NOT affect receiving)
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

	// Track what the client receives on each channel
	receivedMu := &sync.Mutex{}
	receivedChannels := map[string]int{}

	// Subscribe on client to track messages
	for _, ch := range []string{"alerts", "metrics"} {
		ch := ch // capture for closure
		go func() {
			err := clientMsg.Subscribe(ctx, ch, "tracker-"+ch, func(ctx context.Context, msg Message) error {
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

	// Test: Hub publishes to subscribed channels
	t.Log("Hub publishing to 'alerts' channel")
	err = hubMsg.Publish(ctx, "alerts", "test.v1", map[string]any{"msg": "alert-data"})
	require.NoError(t, err)
	time.Sleep(400 * time.Millisecond)

	t.Log("Hub publishing to 'metrics' channel")
	err = hubMsg.Publish(ctx, "metrics", "test.v1", map[string]any{"msg": "metric-data"})
	require.NoError(t, err)
	time.Sleep(400 * time.Millisecond)

	receivedMu.Lock()
	defer receivedMu.Unlock()

	// Verify results
	t.Logf("Client received: alerts=%d, metrics=%d", receivedChannels["alerts"], receivedChannels["metrics"])

	// With the BUG: client's receive policy = publish channels ["events", "logs"]
	// Hub tries to send on ["alerts", "metrics"]
	// Result: policy violation, 0 messages received
	//
	// With the FIX: client's receive policy = subscribe channels ["alerts", "metrics"]
	// Hub sends on ["alerts", "metrics"]
	// Result: all messages received

	assert.Greater(t, receivedChannels["alerts"], 0, "client should receive message on subscribed 'alerts' channel")
	assert.Greater(t, receivedChannels["metrics"], 0, "client should receive message on subscribed 'metrics' channel")

	if receivedChannels["alerts"] == 0 || receivedChannels["metrics"] == 0 {
		t.Errorf("BUG CONFIRMED: Client did not receive messages on subscribed channels. "+
			"The client's receive policy was incorrectly using the Publish channels instead of Subscribe channels. "+
			"alerts=%d (expected >0), metrics=%d (expected >0)",
			receivedChannels["alerts"], receivedChannels["metrics"])
	} else {
		t.Log("PASS: Client received all messages on subscribed channels")
	}
}
