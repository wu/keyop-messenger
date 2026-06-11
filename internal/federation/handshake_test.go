package federation_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/federation"
)

// newConnPair creates a pair of Conn connections backed by an in-process
// net.Pipe. Both connections are closed via t.Cleanup.
func newConnPair(t *testing.T) (server, client *federation.Conn) {
	t.Helper()
	c1, c2 := net.Pipe()
	t.Cleanup(func() {
		_ = c1.Close()
		_ = c2.Close()
	})
	server = federation.NewConn(c1, c1, c1.RemoteAddr(), func() error { return c1.Close() })
	client = federation.NewConn(c2, c2, c2.RemoteAddr(), func() error { return c2.Close() })
	return
}

// TestHandshakeRoundTrip verifies that a HandshakeMsg survives a send/receive
// cycle over a live connection.
func TestHandshakeRoundTrip(t *testing.T) {
	srv, cli := newConnPair(t)

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
// net.Pipe is unbuffered, so each side must send and receive concurrently.
func TestHandshakeBidirectional(t *testing.T) {
	srv, cli := newConnPair(t)

	srvMsg := federation.HandshakeMsg{InstanceName: "hub-server", Role: "hub", Version: "1"}
	cliMsg := federation.HandshakeMsg{InstanceName: "hub-client", Role: "hub", Version: "1"}

	type result struct {
		msg federation.HandshakeMsg
		err error
	}
	srvCh := make(chan result, 1)
	cliCh := make(chan result, 1)

	// Each side sends asynchronously and receives synchronously so the pipe
	// drains: srv's send unblocks when cli's receive reads from it, and vice versa.
	go func() {
		go func() { _ = federation.SendHandshake(srv, srvMsg) }()
		msg, err := federation.ReceiveHandshake(srv)
		srvCh <- result{msg, err}
	}()
	go func() {
		go func() { _ = federation.SendHandshake(cli, cliMsg) }()
		msg, err := federation.ReceiveHandshake(cli)
		cliCh <- result{msg, err}
	}()

	srvResult := <-srvCh
	cliResult := <-cliCh

	require.NoError(t, srvResult.err)
	require.NoError(t, cliResult.err)
	assert.Equal(t, cliMsg.InstanceName, srvResult.msg.InstanceName)
	assert.Equal(t, srvMsg.InstanceName, cliResult.msg.InstanceName)
}

// TestAckRoundTrip verifies that an AckMsg survives a send/receive cycle.
func TestAckRoundTrip(t *testing.T) {
	srv, cli := newConnPair(t)

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

// TestReceiveHandshakeReadError verifies that a network-level read failure
// (peer closed the connection) is propagated with context.
func TestReceiveHandshakeReadError(t *testing.T) {
	srv, cli := newConnPair(t)

	require.NoError(t, cli.Close())

	_, err := federation.ReceiveHandshake(srv)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "receive handshake")
}

// TestReceiveHandshakeWrongFrameType verifies that a binary frame
// is rejected with an "expected JSON frame" error.
func TestReceiveHandshakeWrongFrameType(t *testing.T) {
	srv, cli := newConnPair(t)

	errc := make(chan error, 1)
	go func() { errc <- cli.WriteMessage(federation.MsgTypeBinary, []byte(`{"instance_name":"x"}`)) }()

	_, err := federation.ReceiveHandshake(srv)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected JSON frame")
	require.NoError(t, <-errc)
}

// TestReceiveHandshakeBadJSON verifies that a JSON frame containing invalid
// JSON returns an unmarshal error.
func TestReceiveHandshakeBadJSON(t *testing.T) {
	srv, cli := newConnPair(t)

	errc := make(chan error, 1)
	go func() { errc <- cli.WriteMessage(federation.MsgTypeJSON, []byte("{not valid json")) }()

	_, err := federation.ReceiveHandshake(srv)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal handshake")
	require.NoError(t, <-errc)
}

// TestSendHandshakeWriteError verifies that SendHandshake propagates a write
// error when the underlying connection is already closed.
func TestSendHandshakeWriteError(t *testing.T) {
	_, cli := newConnPair(t)

	require.NoError(t, cli.Close())

	err := federation.SendHandshake(cli, federation.HandshakeMsg{InstanceName: "x", Role: "client", Version: "1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send handshake")
}

// TestReceiveAckReadError verifies that a network-level read failure is
// propagated with context.
func TestReceiveAckReadError(t *testing.T) {
	srv, cli := newConnPair(t)

	require.NoError(t, cli.Close())

	_, err := federation.ReceiveAck(srv)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "receive ack")
}

// TestReceiveAckWrongFrameType verifies that a binary frame is rejected.
func TestReceiveAckWrongFrameType(t *testing.T) {
	srv, cli := newConnPair(t)

	errc := make(chan error, 1)
	go func() { errc <- cli.WriteMessage(federation.MsgTypeBinary, []byte(`{"last_id":"x"}`)) }()

	_, err := federation.ReceiveAck(srv)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected JSON frame")
	require.NoError(t, <-errc)
}

// TestReceiveAckBadJSON verifies that a JSON frame with invalid JSON returns
// an unmarshal error.
func TestReceiveAckBadJSON(t *testing.T) {
	srv, cli := newConnPair(t)

	errc := make(chan error, 1)
	go func() { errc <- cli.WriteMessage(federation.MsgTypeJSON, []byte("{bad json")) }()

	_, err := federation.ReceiveAck(srv)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal ack")
	require.NoError(t, <-errc)
}

// TestSendAckWriteError verifies that SendAck propagates a write error when
// the connection is already closed.
func TestSendAckWriteError(t *testing.T) {
	_, cli := newConnPair(t)

	require.NoError(t, cli.Close())

	err := federation.SendAck(cli, federation.AckMsg{LastID: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "send ack")
}

// TestAckEmptyLastID verifies zero-value AckMsg round-trips cleanly.
func TestAckEmptyLastID(t *testing.T) {
	srv, cli := newConnPair(t)

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
