package federation_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/keyop/keyop-messenger/internal/federation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

// newWSPair creates a pair of WebSocket connections (server, client) backed by
// an in-process httptest server. Both connections are closed via t.Cleanup.
func newWSPair(t *testing.T) (server, client *websocket.Conn) {
	t.Helper()

	ready := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		require.NoError(t, err)
		ready <- conn
	}))
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })

	serverConn := <-ready
	t.Cleanup(func() { _ = serverConn.Close() })

	return serverConn, clientConn
}

// TestHandshakeRoundTrip verifies that a HandshakeMsg survives a send/receive
// cycle over a live WebSocket connection.
func TestHandshakeRoundTrip(t *testing.T) {
	srv, cli := newWSPair(t)

	sent := federation.HandshakeMsg{
		InstanceName: "hub-east",
		Role:         "hub",
		Version:      "1",
	}

	errc := make(chan error, 1)
	var received federation.HandshakeMsg
	go func() {
		var err error
		received, err = federation.ReceiveHandshake(srv)
		errc <- err
	}()

	require.NoError(t, federation.SendHandshake(cli, sent))
	require.NoError(t, <-errc)

	assert.Equal(t, sent.InstanceName, received.InstanceName)
	assert.Equal(t, sent.Role, received.Role)
	assert.Equal(t, sent.Version, received.Version)
}

// TestHandshakeBidirectional verifies the exchange works in both directions.
func TestHandshakeBidirectional(t *testing.T) {
	srv, cli := newWSPair(t)

	srvMsg := federation.HandshakeMsg{InstanceName: "hub-server", Role: "hub", Version: "1"}
	cliMsg := federation.HandshakeMsg{InstanceName: "hub-client", Role: "hub", Version: "1"}

	// Both sides send concurrently, then receive.
	errSrv := make(chan error, 1)
	errCli := make(chan error, 1)
	go func() { errSrv <- federation.SendHandshake(srv, srvMsg) }()
	go func() { errCli <- federation.SendHandshake(cli, cliMsg) }()
	require.NoError(t, <-errSrv)
	require.NoError(t, <-errCli)

	gotBySrv, err := federation.ReceiveHandshake(srv)
	require.NoError(t, err)
	gotByCli, err := federation.ReceiveHandshake(cli)
	require.NoError(t, err)

	assert.Equal(t, cliMsg.InstanceName, gotBySrv.InstanceName)
	assert.Equal(t, srvMsg.InstanceName, gotByCli.InstanceName)
}

// TestAckRoundTrip verifies that an AckMsg survives a send/receive cycle.
func TestAckRoundTrip(t *testing.T) {
	srv, cli := newWSPair(t)

	sent := federation.AckMsg{LastID: "01J0000000000000000000000A"}

	errc := make(chan error, 1)
	var received federation.AckMsg
	go func() {
		var err error
		received, err = federation.ReceiveAck(srv)
		errc <- err
	}()

	require.NoError(t, federation.SendAck(cli, sent))
	require.NoError(t, <-errc)

	assert.Equal(t, sent.LastID, received.LastID)
}

// TestAckEmptyLastID verifies zero-value AckMsg round-trips cleanly.
func TestAckEmptyLastID(t *testing.T) {
	srv, cli := newWSPair(t)

	errc := make(chan error, 1)
	var received federation.AckMsg
	go func() {
		var err error
		received, err = federation.ReceiveAck(srv)
		errc <- err
	}()

	require.NoError(t, federation.SendAck(cli, federation.AckMsg{}))
	require.NoError(t, <-errc)
	assert.Equal(t, "", received.LastID)
}
