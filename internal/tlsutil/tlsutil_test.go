package tlsutil_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/testutil"
	"github.com/wu/keyop-messenger/internal/tlsutil"
)

// ---- helpers -----------------------------------------------------------------

func writePEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, data, 0o600))
	return p
}

func parseCertPEM(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block, "expected PEM block in cert")
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	return cert
}

func parseKeyPEM(t *testing.T, keyPEM []byte) *ecdsa.PrivateKey {
	t.Helper()
	block, _ := pem.Decode(keyPEM)
	require.NotNil(t, block, "expected PEM block in key")
	key, err := x509.ParseECPrivateKey(block.Bytes)
	require.NoError(t, err)
	return key
}

// ---- keygen ------------------------------------------------------------------

func TestGenerateCA(t *testing.T) {
	certPEM, keyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)

	cert := parseCertPEM(t, certPEM)
	key := parseKeyPEM(t, keyPEM)

	assert.True(t, cert.IsCA, "cert should be a CA")
	assert.True(t, cert.BasicConstraintsValid)
	assert.Equal(t, elliptic.P384(), key.Curve, "key should be P-384")
	assert.WithinDuration(t, time.Now().Add(365*24*time.Hour), cert.NotAfter, 5*time.Second)
}

func TestGenerateInstance(t *testing.T) {
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)

	const name = "billing-hub"
	certPEM, keyPEM, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, name, 90)
	require.NoError(t, err)

	cert := parseCertPEM(t, certPEM)
	key := parseKeyPEM(t, keyPEM)

	assert.Equal(t, name, cert.Subject.CommonName)
	assert.Contains(t, cert.DNSNames, name)
	assert.False(t, cert.IsCA)
	assert.Equal(t, elliptic.P384(), key.Curve)

	// Chain verification against the CA.
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(caCertPEM)
	_, err = cert.Verify(x509.VerifyOptions{Roots: caPool, DNSName: name})
	require.NoError(t, err, "instance cert should verify against CA")
}

func TestGenerateInstanceInvalidCA(t *testing.T) {
	_, _, err := tlsutil.GenerateInstance([]byte("not-pem"), []byte("not-pem"), "x", 1)
	assert.Error(t, err)
}

// ---- BuildTLSConfig ----------------------------------------------------------

func TestBuildTLSConfig(t *testing.T) {
	dir := t.TempDir()
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)

	certPEM, keyPEM, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "hub1", 90)
	require.NoError(t, err)

	certFile := writePEM(t, dir, "hub1.crt", certPEM)
	keyFile := writePEM(t, dir, "hub1.key", keyPEM)
	caFile := writePEM(t, dir, "ca.crt", caCertPEM)

	logger := &testutil.FakeLogger{}
	cfg, err := tlsutil.BuildTLSConfig(certFile, keyFile, caFile, logger)
	require.NoError(t, err)

	assert.Equal(t, uint16(tls.VersionTLS13), cfg.MinVersion)
	assert.Equal(t, tls.RequireAndVerifyClientCert, cfg.ClientAuth)
	assert.NotNil(t, cfg.ClientCAs)
	assert.Len(t, cfg.Certificates, 1)
}

func TestBuildTLSConfigMissingCert(t *testing.T) {
	dir := t.TempDir()
	caCertPEM, _, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)
	caFile := writePEM(t, dir, "ca.crt", caCertPEM)

	logger := &testutil.FakeLogger{}
	_, err = tlsutil.BuildTLSConfig(
		filepath.Join(dir, "missing.crt"),
		filepath.Join(dir, "missing.key"),
		caFile,
		logger,
	)
	assert.Error(t, err)
}

func TestBuildTLSConfigMissingCA(t *testing.T) {
	dir := t.TempDir()
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)
	certPEM, keyPEM, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "hub1", 90)
	require.NoError(t, err)

	certFile := writePEM(t, dir, "hub1.crt", certPEM)
	keyFile := writePEM(t, dir, "hub1.key", keyPEM)

	logger := &testutil.FakeLogger{}
	_, err = tlsutil.BuildTLSConfig(certFile, keyFile, filepath.Join(dir, "missing-ca.crt"), logger)
	assert.Error(t, err)
}

// ---- ExtractCN / CheckExpiry -------------------------------------------------

func TestExtractCN(t *testing.T) {
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)
	certPEM, _, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "my-hub", 90)
	require.NoError(t, err)

	cert := parseCertPEM(t, certPEM)
	assert.Equal(t, "my-hub", tlsutil.ExtractCN(cert))
}

func TestCheckExpiryWarns(t *testing.T) {
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)

	// Generate a cert expiring in 15 days — below the 30-day warn threshold.
	certPEM, _, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "hub1", 15)
	require.NoError(t, err)

	cert := parseCertPEM(t, certPEM)
	logger := &testutil.FakeLogger{}
	tlsutil.CheckExpiry(cert, 30, logger)

	assert.True(t, logger.HasWarn("expiring"), "expected expiry warning in logger")
}

func TestCheckExpiryNoWarn(t *testing.T) {
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)

	// Cert expiring in 90 days — above the 30-day warn threshold.
	certPEM, _, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "hub1", 90)
	require.NoError(t, err)

	cert := parseCertPEM(t, certPEM)
	logger := &testutil.FakeLogger{}
	tlsutil.CheckExpiry(cert, 30, logger)

	assert.False(t, logger.HasWarn("expiring"))
}

// ---- HotReloadTLS ------------------------------------------------------------

func TestHotReloadTLS(t *testing.T) {
	dir := t.TempDir()
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)
	caFile := writePEM(t, dir, "ca.crt", caCertPEM)

	// Write initial instance cert.
	certPEM1, keyPEM1, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "hub1", 90)
	require.NoError(t, err)
	certFile := writePEM(t, dir, "hub1.crt", certPEM1)
	keyFile := writePEM(t, dir, "hub1.key", keyPEM1)

	watcher := &testutil.FakeChannelWatcher{}
	logger := &testutil.FakeLogger{}

	h, err := tlsutil.HotReloadTLSConfig(certFile, keyFile, caFile, watcher, logger)
	require.NoError(t, err)
	defer h.Close()

	// GetConfigForClient should return the initial cert's serial.
	outerCfg := h.Config()
	cfg1, err := outerCfg.GetConfigForClient(nil)
	require.NoError(t, err)
	require.Len(t, cfg1.Certificates, 1)
	leaf1, err := x509.ParseCertificate(cfg1.Certificates[0].Certificate[0])
	require.NoError(t, err)
	serial1 := leaf1.SerialNumber

	// Generate and write a new cert.
	certPEM2, keyPEM2, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "hub1", 90)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certFile, certPEM2, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM2, 0o600))

	// Trigger the watcher notification on certFile.
	watcher.Notify(certFile)

	// Wait for reload — assert new serial within 1 second.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		cfg2, err := outerCfg.GetConfigForClient(nil)
		require.NoError(t, err)
		leaf2, err := x509.ParseCertificate(cfg2.Certificates[0].Certificate[0])
		require.NoError(t, err)
		if leaf2.SerialNumber.Cmp(serial1) != 0 {
			return // reload observed
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("TLS config was not reloaded within 1 second")
}

func TestHotReloadTLSClose(t *testing.T) {
	dir := t.TempDir()
	caCertPEM, caKeyPEM, err := tlsutil.GenerateCA(365)
	require.NoError(t, err)
	certPEM, keyPEM, err := tlsutil.GenerateInstance(caCertPEM, caKeyPEM, "hub1", 90)
	require.NoError(t, err)

	certFile := writePEM(t, dir, "hub1.crt", certPEM)
	keyFile := writePEM(t, dir, "hub1.key", keyPEM)
	caFile := writePEM(t, dir, "ca.crt", caCertPEM)

	watcher := &testutil.FakeChannelWatcher{}
	h, err := tlsutil.HotReloadTLSConfig(certFile, keyFile, caFile, watcher, &testutil.FakeLogger{})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() { h.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() timed out")
	}
}
