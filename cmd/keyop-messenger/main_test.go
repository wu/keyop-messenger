package main

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runCmd creates a fresh root command, injects args, captures stdout+stderr,
// and returns the combined output and execution error.
func runCmd(args ...string) (out string, err error) {
	cmd := rootCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return buf.String(), err
}

func TestKeygenCA(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")

	out, err := runCmd("keygen", "ca",
		"--out-cert", certPath,
		"--out-key", keyPath,
		"--days", "365",
	)
	require.NoError(t, err)

	// Output must contain the standard cert info fields.
	assert.Contains(t, out, "Subject:")
	assert.Contains(t, out, "Valid from:")
	assert.Contains(t, out, "Valid until:")
	assert.Contains(t, out, "Fingerprint: SHA-256:")

	// Cert file must parse as a CA certificate.
	certPEM, err := os.ReadFile(certPath)
	require.NoError(t, err)
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block, "cert file must contain a PEM block")
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	assert.True(t, cert.IsCA, "generated cert must be a CA")

	// Key file must parse as a P-384 EC private key.
	keyPEM, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	keyBlock, _ := pem.Decode(keyPEM)
	require.NotNil(t, keyBlock, "key file must contain a PEM block")
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	require.NoError(t, err)
	assert.Equal(t, "P-384", key.Curve.Params().Name)
}

func TestKeygenInstance(t *testing.T) {
	dir := t.TempDir()

	// Generate a CA first.
	caPath := filepath.Join(dir, "ca.crt")
	caKeyPath := filepath.Join(dir, "ca.key")
	_, err := runCmd("keygen", "ca", "--out-cert", caPath, "--out-key", caKeyPath)
	require.NoError(t, err)

	// Generate an instance cert signed by the CA.
	certPath := filepath.Join(dir, "billing.crt")
	keyPath := filepath.Join(dir, "billing.key")
	out, err := runCmd("keygen", "instance",
		"--ca", caPath,
		"--ca-key", caKeyPath,
		"--name", "billing-host",
		"--out-cert", certPath,
		"--out-key", keyPath,
	)
	require.NoError(t, err)
	assert.Contains(t, out, "billing-host")

	// Cert must have CN=billing-host.
	certPEM, err := os.ReadFile(certPath)
	require.NoError(t, err)
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	cert, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
	assert.Equal(t, "billing-host", cert.Subject.CommonName)

	// Cert must verify against the CA.
	caCertPEM, err := os.ReadFile(caPath)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCertPEM)
	_, verifyErr := cert.Verify(x509.VerifyOptions{
		Roots:   pool,
		DNSName: "billing-host",
	})
	assert.NoError(t, verifyErr)
}

func TestKeygenCARefuseOverwrite(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")

	// Create a file at the cert output path.
	require.NoError(t, os.WriteFile(certPath, []byte("existing"), 0o600))

	_, err := runCmd("keygen", "ca", "--out-cert", certPath, "--out-key", keyPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Original file must be unchanged.
	content, readErr := os.ReadFile(certPath)
	require.NoError(t, readErr)
	assert.Equal(t, "existing", string(content))
}

func TestKeygenCAForceOverwrite(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "ca.crt")
	keyPath := filepath.Join(dir, "ca.key")

	// Place a sentinel value in the cert file.
	require.NoError(t, os.WriteFile(certPath, []byte("old"), 0o600))

	_, err := runCmd("keygen", "ca",
		"--out-cert", certPath,
		"--out-key", keyPath,
		"--force",
	)
	require.NoError(t, err)

	content, readErr := os.ReadFile(certPath)
	require.NoError(t, readErr)
	assert.NotEqual(t, "old", string(content), "--force must overwrite the existing file")
}

func TestKeygenInstanceMissingRequiredFlags(t *testing.T) {
	_, err := runCmd("keygen", "instance")
	require.Error(t, err, "must fail when --ca, --ca-key, and --name are not provided")
}

func TestKeygenInstanceDefaultOutputPaths(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	caKeyPath := filepath.Join(dir, "ca.key")

	_, err := runCmd("keygen", "ca", "--out-cert", caPath, "--out-key", caKeyPath)
	require.NoError(t, err)

	// Change CWD so default output paths resolve inside the temp dir.
	orig, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(orig) })

	_, err = runCmd("keygen", "instance",
		"--ca", caPath,
		"--ca-key", caKeyPath,
		"--name", "myhost",
	)
	require.NoError(t, err)

	assert.FileExists(t, filepath.Join(dir, "myhost.crt"))
	assert.FileExists(t, filepath.Join(dir, "myhost.key"))
}

func TestVersionCmd(t *testing.T) {
	out, err := runCmd("version")
	require.NoError(t, err)
	assert.Contains(t, out, "version:")
}
