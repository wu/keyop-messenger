//go:build integration

package federation

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// tlsFixture holds two independent CAs so tests can build certificates that a
// peer trusts or does not.
type tlsFixture struct {
	t               *testing.T
	dir             string
	ca1Cert, ca1Key []byte
	ca2Cert, ca2Key []byte
}

func newTLSFixture(t *testing.T) *tlsFixture {
	t.Helper()
	f := &tlsFixture{t: t, dir: t.TempDir()}
	var err error
	f.ca1Cert, f.ca1Key, err = tlsutil.GenerateCA(365)
	require.NoError(t, err)
	f.ca2Cert, f.ca2Key, err = tlsutil.GenerateCA(365)
	require.NoError(t, err)
	return f
}

// config builds a BuildTLSConfig config for name, with its certificate signed
// by signCA and trusting trustCA.
func (f *tlsFixture) config(name string, signCert, signKey, trustCA []byte) *tls.Config {
	f.t.Helper()
	certPEM, keyPEM, err := tlsutil.GenerateInstance(signCert, signKey, name, 90)
	require.NoError(f.t, err)
	write := func(file string, data []byte) string {
		p := filepath.Join(f.dir, name+"-"+file)
		require.NoError(f.t, os.WriteFile(p, data, 0o600))
		return p
	}
	cfg, err := tlsutil.BuildTLSConfig(write("crt", certPEM), write("key", keyPEM), write("ca", trustCA), &testutil.FakeLogger{})
	require.NoError(f.t, err)
	return cfg
}

func startTLSHub(t *testing.T, hubTLS *tls.Config) string {
	t.Helper()
	dd, _ := dedup.NewLRUDedup(100)
	hub := NewHub(HubConfig{AllowedPeers: []AllowedPeer{{Name: "client"}}}, hubTLS, "",
		func([]*envelope.Envelope) error { return nil }, dd, noopAuditLogger{}, &testutil.FakeLogger{}, 100, 65536, "")
	require.NoError(t, hub.Listen("127.0.0.1:0"))
	t.Cleanup(func() { _ = hub.Close() })
	return hub.Addr()
}

func newTLSTestClient(t *testing.T, clientTLS *tls.Config) *Client {
	t.Helper()
	dd, _ := dedup.NewLRUDedup(100)
	c := NewClient("client", clientTLS, NewAtomicPolicy(ForwardPolicy{}),
		nil, nil, dd, noopAuditLogger{}, &testutil.FakeLogger{},
		65536, 100*time.Millisecond, 500*time.Millisecond, 0.1, nil, nil, "")
	t.Cleanup(c.Close)
	return c
}

func TestClientDial_ValidMTLS_Succeeds(t *testing.T) {
	t.Parallel()
	f := newTLSFixture(t)
	addr := startTLSHub(t, f.config("hub", f.ca1Cert, f.ca1Key, f.ca1Cert))
	c := newTLSTestClient(t, f.config("client", f.ca1Cert, f.ca1Key, f.ca1Cert))

	require.NoError(t, c.Dial(addr))
	assert.True(t, c.Connected())
}

func TestClientDial_UntrustedHubCert_IsFatal(t *testing.T) {
	t.Parallel()
	f := newTLSFixture(t)
	// Hub cert signed by CA2; the client trusts only CA1.
	addr := startTLSHub(t, f.config("hub", f.ca2Cert, f.ca2Key, f.ca1Cert))
	c := newTLSTestClient(t, f.config("client", f.ca1Cert, f.ca1Key, f.ca1Cert))

	err := c.Dial(addr)
	require.Error(t, err)
	assert.True(t, isFatalConnErr(err), "untrusted hub cert must be fatal: %v", err)
}

func TestClientDial_RejectedClientCert_IsFatal(t *testing.T) {
	t.Parallel()
	f := newTLSFixture(t)
	// Client cert signed by CA2; the hub trusts only CA1. Under TLS 1.3 the
	// hub's rejection arrives after the client's handshake completes.
	addr := startTLSHub(t, f.config("hub", f.ca1Cert, f.ca1Key, f.ca1Cert))
	c := newTLSTestClient(t, f.config("client", f.ca2Cert, f.ca2Key, f.ca1Cert))

	err := c.Dial(addr)
	require.Error(t, err)
	assert.True(t, isFatalConnErr(err), "client cert rejected by hub must be fatal: %v", err)
}

func TestClientDial_HubDown_IsNotFatal(t *testing.T) {
	t.Parallel()
	f := newTLSFixture(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	c := newTLSTestClient(t, f.config("client", f.ca1Cert, f.ca1Key, f.ca1Cert))

	err = c.Dial(addr)
	require.Error(t, err)
	assert.False(t, isFatalConnErr(err), "connection refused must be transient: %v", err)
}

func TestEphemeralConnect_UntrustedHubCert_IsFatal(t *testing.T) {
	t.Parallel()
	f := newTLSFixture(t)
	addr := startTLSHub(t, f.config("hub", f.ca2Cert, f.ca2Key, f.ca1Cert))
	ec := NewEphemeralClient(EphemeralClientConfig{
		InstanceName: "client",
		TLSConfig:    f.config("client", f.ca1Cert, f.ca1Key, f.ca1Cert),
	}, &testutil.FakeLogger{})
	t.Cleanup(ec.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := ec.Connect(ctx, addr)
	require.Error(t, err)
	assert.True(t, isFatalConnErr(err), "untrusted hub cert must be fatal: %v", err)
}
