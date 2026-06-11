package federation_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/federation"
)

// findAuditEvent returns the first recorded event with the given name.
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

// TestHubClientConnected_AuditIncludesPeerAddr verifies that the client_connected
// audit event records the remote peer address.
func TestHubClientConnected_AuditIncludesPeerAddr(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "peer-addr-test"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newConnPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "peer-addr-test")

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.NotEmpty(t, evt.PeerAddr, "client_connected should record the remote address in PeerAddr")
	assert.Contains(t, evt.Detail, "addr=", "client_connected Detail should contain addr=")
}

// TestHubClientConnected_AuditDetailIncludesPublishChannels verifies that the
// client_connected audit event Detail includes the publish channel allowlist.
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

	srv, cli := newConnPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "pub-ch-test")

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "pub=", "client_connected Detail should contain pub=")
	assert.Contains(t, evt.Detail, "orders", "Detail should list the orders channel")
	assert.Contains(t, evt.Detail, "events", "Detail should list the events channel")
}
