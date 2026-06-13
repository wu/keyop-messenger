//go:build integration

package federation_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/federation"
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
