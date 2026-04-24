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

	srv, cli := newWSPair(t)
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

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "pub-ch-test")

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "pub=[orders,events]",
		"client_connected Detail should list the publish channels")
}

// TestHubClientConnected_AuditDetailNoChannels verifies that the Detail does not
// include a pub= or sub= segment when the peer has no channel restrictions.
func TestHubClientConnected_AuditDetailNoChannels(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{{Name: "no-channels"}},
	}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "no-channels")

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventClientConnected)
	}, time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventClientConnected)
	require.True(t, found)
	// addr= must be present, but no pub= or sub= when channels are unrestricted.
	assert.Contains(t, evt.Detail, "addr=")
	assert.NotContains(t, evt.Detail, "pub=")
	assert.NotContains(t, evt.Detail, "sub=")
}

// TestHubPeerDisconnected_AuditIncludesPeerAddr verifies that the peer_disconnected
// audit event records the remote address.
func TestHubPeerDisconnected_AuditIncludesPeerAddr(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "disconn-addr-test"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "disconn-addr-test")

	time.Sleep(50 * time.Millisecond)
	_ = cli.Close()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventPeerDisconnected)
	require.True(t, found)
	assert.NotEmpty(t, evt.PeerAddr, "peer_disconnected should record PeerAddr")
}

// TestHubPeerDisconnected_AuditDetailIncludesDuration verifies that the
// peer_disconnected Detail includes the connection duration.
func TestHubPeerDisconnected_AuditDetailIncludesDuration(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "disconn-dur-test"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "disconn-dur-test")

	time.Sleep(50 * time.Millisecond)
	_ = cli.Close()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventPeerDisconnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "duration=",
		"peer_disconnected Detail should contain the connection duration")
}

// TestHubPeerDisconnected_AuditDetailIncludesError verifies that the peer_disconnected
// Detail includes an err= field when the connection is dropped.
func TestHubPeerDisconnected_AuditDetailIncludesError(t *testing.T) {
	auditL := &fakeAuditLog{}
	cw := &countingWriter{}
	cfg := federation.HubConfig{AllowedPeers: []federation.AllowedPeer{{Name: "disconn-err-test"}}}
	hub := newHub(t, cfg, cw, auditL)

	srv, cli := newWSPair(t)
	hub.ServeTestConn(srv, nil)
	clientHandshake(t, cli, "disconn-err-test")

	time.Sleep(50 * time.Millisecond)
	// Force-close the underlying TCP connection without a WebSocket close frame.
	_ = cli.UnderlyingConn().Close()

	require.Eventually(t, func() bool {
		return auditL.has(audit.EventPeerDisconnected)
	}, 2*time.Second, 20*time.Millisecond)

	evt, found := findAuditEvent(auditL, audit.EventPeerDisconnected)
	require.True(t, found)
	assert.Contains(t, evt.Detail, "err=",
		"peer_disconnected Detail should contain err= after an abrupt close")
}
