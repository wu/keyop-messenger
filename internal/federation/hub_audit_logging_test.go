//go:build integration

package federation_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/federation"
	"google.golang.org/grpc/metadata"
)

func findAuditEvent(auditL *fakeAuditLog, name string) (audit.Event, bool) {
	auditL.mu.Lock()
	defer auditL.mu.Unlock()
	for _, e := range auditL.events {
		if e.Event == name {
			return e, true
		}
	}
	return audit.Event{}, false
}

func TestHubClientConnected_AuditIncludesPeerAddr(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "peer-addr-test"}}}
	hub := newHub(t, cfg, cw, auditL)

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	_, cancel := openPublish(t, stub, "peer-addr-test")
	defer cancel()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.NotEmpty(t, evt.PeerAddr, "client_connected should record the remote address in PeerAddr")
	assert.Contains(t, evt.Detail, "addr=", "client_connected Detail should contain addr=")
}

func TestHubClientConnected_AuditDetailIncludesPublishChannels(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{
			Name:    "pub-ch-test",
			Publish: []string{"orders", "events"},
		}},
	}
	hub := newHub(t, cfg, cw, auditL)

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	_, cancel := openPublish(t, stub, "pub-ch-test")
	defer cancel()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "pub=", "client_connected Detail should contain pub=")
	assert.Contains(t, evt.Detail, "orders", "Detail should list the orders channel")
	assert.Contains(t, evt.Detail, "events", "Detail should list the events channel")
}

// TestHubClientConnected_AuditUsesDeclaredPublishChannels verifies that when a
// client declares its publish channels via metadata, the hub reports those (not
// the configured allowlist) in the connection audit detail. The hub here has no
// publish allowlist, so the declared channels are the only possible source.
func TestHubClientConnected_AuditUsesDeclaredPublishChannels(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "declarer"}}, // no Publish allowlist
	}
	hub := newHub(t, cfg, cw, auditL)

	stub, cleanup := startHub(t, hub)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	md := metadata.Pairs(
		"x-federation-instance", "declarer",
		"x-federation-publish-channels", "alpha",
		"x-federation-publish-channels", "beta",
	)
	_, err := stub.Publish(metadata.NewOutgoingContext(ctx, md))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "pub=", "Detail should contain pub= from the declared channels")
	assert.Contains(t, evt.Detail, "alpha", "Detail should list the declared alpha channel")
	assert.Contains(t, evt.Detail, "beta", "Detail should list the declared beta channel")
}
