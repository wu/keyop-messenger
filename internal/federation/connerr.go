package federation

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// fatalTLSAlerts are the TLS alerts (RFC 8446 §6) that mean the handshake or
// this instance's certificate was rejected, so retrying cannot succeed without
// operator action. crypto/tls does not export its alert constants.
var fatalTLSAlerts = []tls.AlertError{
	40,  // handshake_failure
	42,  // bad_certificate
	43,  // unsupported_certificate
	44,  // certificate_revoked
	45,  // certificate_expired
	46,  // certificate_unknown
	48,  // unknown_ca
	116, // certificate_required
}

// isFatalConnErr reports whether err is a non-retryable connection failure —
// one where reconnecting cannot succeed without operator action:
//
//   - the hub rejecting this client's identity: PermissionDenied (CN not in the
//     allowlist), Unauthenticated, or InvalidArgument (no usable CN, or a
//     protocol violation);
//   - a TLS failure on certificate grounds, on either side of the connection.
//
// Everything else (connection refused, timeouts, DNS, hub shutting down) is
// transient. gRPC reports TLS failures as codes.Unavailable with the cause
// flattened into a string, so they are only recognized when the typed cause was
// attached by tlsFailureRecorder.annotate.
func isFatalConnErr(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	if code == codes.PermissionDenied || code == codes.Unauthenticated || code == codes.InvalidArgument {
		return true
	}
	return isFatalTLSErr(err)
}

// isFatalTLSErr reports whether err carries a TLS failure on certificate
// grounds: this side rejecting the peer's certificate, or the peer rejecting
// the handshake or this side's certificate with an alert.
func isFatalTLSErr(err error) bool {
	var (
		peerErr      *tlsutil.PeerVerificationError
		certErr      *tls.CertificateVerificationError
		authorityErr x509.UnknownAuthorityError
		invalidErr   x509.CertificateInvalidError
		hostnameErr  x509.HostnameError
		alertErr     tls.AlertError
	)
	switch {
	case errors.As(err, &peerErr), errors.As(err, &certErr), errors.As(err, &authorityErr),
		errors.As(err, &invalidErr), errors.As(err, &hostnameErr):
		return true
	case errors.As(err, &alertErr):
		return isFatalTLSAlert(alertErr.Error())
	}
	// crypto/tls reports an alert received from the peer as a *net.OpError with
	// Op "remote error" wrapping its unexported alert type, whose text is the
	// same as AlertError's for the same code.
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "remote error" && opErr.Err != nil {
		return isFatalTLSAlert(opErr.Err.Error())
	}
	return false
}

// isFatalTLSAlert reports whether msg is the text of one of fatalTLSAlerts.
func isFatalTLSAlert(msg string) bool {
	for _, a := range fatalTLSAlerts {
		if msg == a.Error() {
			return true
		}
	}
	return false
}

// tlsFailureRecorder remembers the most recent fatal TLS failure on one gRPC
// connection's transports. gRPC dials lazily and in the background, and turns a
// transport failure into a codes.Unavailable status whose description is a
// string, so the typed cause is captured at the credentials layer instead and
// re-attached to stream errors by annotate. The record is cleared once a
// transport reads data from the hub, i.e. once TLS is known to have succeeded.
type tlsFailureRecorder struct {
	mu  sync.Mutex
	err error
}

// observe records err if it is a fatal TLS failure; other errors are ignored.
func (r *tlsFailureRecorder) observe(err error) {
	if !isFatalTLSErr(err) {
		return
	}
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
}

func (r *tlsFailureRecorder) clear() {
	r.mu.Lock()
	r.err = nil
	r.mu.Unlock()
}

// annotate returns err with the recorded TLS failure, if any, attached as a
// second cause visible to errors.As. The message is unchanged: gRPC's status
// description already includes the failure's text.
func (r *tlsFailureRecorder) annotate(err error) error {
	if err == nil {
		return nil
	}
	r.mu.Lock()
	cause := r.err
	r.mu.Unlock()
	if cause == nil {
		return err
	}
	return &tlsCauseError{err: err, cause: cause}
}

// tlsCauseError is an error annotated with the typed TLS failure behind it.
type tlsCauseError struct {
	err   error
	cause error
}

func (e *tlsCauseError) Error() string { return e.err.Error() }

func (e *tlsCauseError) Unwrap() []error { return []error{e.err, e.cause} }

// failureCapturingConn reports TLS failures that arrive after this side's
// handshake has completed. Under TLS 1.3 the hub verifies the client's
// certificate after the client has finished its handshake, so a rejected client
// certificate surfaces as an alert on the first read rather than as a
// ClientHandshake error.
type failureCapturingConn struct {
	net.Conn
	failures *tlsFailureRecorder
	readOK   atomic.Bool
}

func (c *failureCapturingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.readOK.CompareAndSwap(false, true) {
		c.failures.clear()
	}
	if err != nil {
		c.failures.observe(err)
	}
	return n, err
}
