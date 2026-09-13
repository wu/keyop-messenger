package federation

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// remoteAlert stands in for crypto/tls's unexported alert type, which carries
// the same text as tls.AlertError for the same code but is not an AlertError.
type remoteAlert uint8

func (a remoteAlert) Error() string { return tls.AlertError(a).Error() }

func TestIsFatalConnErr_TLS(t *testing.T) {
	t.Parallel()
	unavailable := status.Error(codes.Unavailable, "connection error: desc = transport: authentication handshake failed")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"invalid argument (no CN)", status.Error(codes.InvalidArgument, "federation: no CN"), true},
		{"peer verification error", fmt.Errorf("handshake: %w", &tlsutil.PeerVerificationError{Err: errors.New("CA presented as leaf")}), true},
		{"certificate verification error", &tls.CertificateVerificationError{Err: errors.New("bad chain")}, true},
		{"unknown authority", x509.UnknownAuthorityError{}, true},
		{"expired certificate", x509.CertificateInvalidError{Reason: x509.Expired}, true},
		{"hostname mismatch", x509.HostnameError{Certificate: &x509.Certificate{}, Host: "hub"}, true},
		{"local alert bad_certificate", tls.AlertError(42), true},
		{"local alert close_notify", tls.AlertError(0), false},
		{"remote alert unknown_ca", &net.OpError{Op: "remote error", Err: remoteAlert(48)}, true},
		{"remote alert certificate_required", &net.OpError{Op: "remote error", Err: remoteAlert(116)}, true},
		{"remote alert close_notify", &net.OpError{Op: "remote error", Err: remoteAlert(0)}, false},
		{"dial refused", &net.OpError{Op: "dial", Err: errors.New("connect: connection refused")}, false},
		{"unavailable without cause", unavailable, false},
		{"unavailable with TLS cause", &tlsCauseError{err: unavailable, cause: x509.UnknownAuthorityError{}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isFatalConnErr(tt.err))
		})
	}
}

func TestTLSFailureRecorder(t *testing.T) {
	t.Parallel()
	streamErr := status.Error(codes.Unavailable, "connection error")

	var r tlsFailureRecorder
	assert.Same(t, streamErr, r.annotate(streamErr), "nothing recorded: error returned unchanged")
	assert.NoError(t, r.annotate(nil))

	r.observe(&net.OpError{Op: "read", Err: errors.New("connection reset by peer")})
	assert.Same(t, streamErr, r.annotate(streamErr), "transient errors are not recorded")

	r.observe(&net.OpError{Op: "remote error", Err: remoteAlert(42)})
	annotated := r.annotate(streamErr)
	assert.Equal(t, streamErr.Error(), annotated.Error(), "annotation keeps the message")
	assert.Equal(t, codes.Unavailable, status.Code(annotated), "annotation keeps the status")
	assert.True(t, isFatalConnErr(annotated))

	r.clear()
	assert.Same(t, streamErr, r.annotate(streamErr))
}

// TestFailureCapturingConn_ClearsOnData verifies a successful read clears a
// recorded failure and a TLS alert on read records one.
func TestFailureCapturingConn(t *testing.T) {
	t.Parallel()
	server, client := net.Pipe()
	defer func() { _ = server.Close() }()

	var r tlsFailureRecorder
	r.observe(x509.UnknownAuthorityError{})
	conn := &failureCapturingConn{Conn: client, failures: &r}

	go func() { _, _ = server.Write([]byte("x")) }()
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.False(t, isFatalConnErr(r.annotate(errors.New("later"))), "data read clears the record")

	alertConn := &failureCapturingConn{Conn: alertReadConn{client}, failures: &r}
	_, err = alertConn.Read(buf)
	assert.Error(t, err)
	assert.True(t, isFatalConnErr(r.annotate(errors.New("later"))), "alert on read is recorded")
}

// alertReadConn is a net.Conn whose reads fail with a received TLS alert.
type alertReadConn struct{ net.Conn }

func (alertReadConn) Read([]byte) (int, error) {
	return 0, &net.OpError{Op: "remote error", Err: remoteAlert(42)}
}
