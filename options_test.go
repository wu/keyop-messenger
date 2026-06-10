package messenger

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWithConfigNilPositional verifies that passing nil as the positional
// Config and supplying one via WithConfig constructs a working Messenger.
func TestWithConfigNilPositional(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Name = "via-option"

	m, err := New(nil, WithConfig(cfg))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, "via-option", m.InstanceName())
}

// TestWithConfigPositionalTakesPrecedence verifies that when both the
// positional Config and WithConfig are provided, the positional arg wins.
func TestWithConfigPositionalTakesPrecedence(t *testing.T) {
	dir := t.TempDir()

	positional := testConfig(dir)
	positional.Name = "positional"

	optionCfg := testConfig(dir)
	optionCfg.Name = "from-option"

	m, err := New(positional, WithConfig(optionCfg))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, "positional", m.InstanceName())
}

// TestWithConfigBothNilErrors verifies that New returns an error when no
// Config is provided either positionally or via WithConfig.
func TestWithConfigBothNilErrors(t *testing.T) {
	_, err := New(nil)
	require.Error(t, err)
}

// TestWithDataDirOverridesConfig verifies that WithDataDir replaces
// Config.Storage.DataDir and that the messenger actually writes data there.
func TestWithDataDirOverridesConfig(t *testing.T) {
	configDir := t.TempDir()
	overrideDir := t.TempDir()

	cfg := testConfig(configDir)
	m, err := New(cfg, WithDataDir(overrideDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// Publishing triggers directory creation under dataDir.
	require.NoError(t, m.Publish(context.Background(), "probe", "test.T", nil))

	// Data should appear under the override dir, not the config dir.
	_, err = os.Stat(filepath.Join(overrideDir, "channels", "probe"))
	assert.NoError(t, err, "channel dir should exist under the override data dir")

	_, err = os.Stat(filepath.Join(configDir, "channels", "probe"))
	assert.True(t, os.IsNotExist(err), "channel dir should NOT exist under the config data dir")
}

// TestWithDataDirWithNilConfig verifies the combination of WithConfig (nil
// positional) and WithDataDir: the data directory from WithDataDir wins over
// the one embedded in the option config.
func TestWithDataDirWithNilConfig(t *testing.T) {
	configDir := t.TempDir()
	overrideDir := t.TempDir()

	cfg := testConfig(configDir)
	cfg.Name = "combo"

	m, err := New(nil, WithConfig(cfg), WithDataDir(overrideDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, "combo", m.InstanceName())

	received := make(chan struct{}, 1)
	require.NoError(t, m.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, _ Message) error {
			received <- struct{}{}
			return nil
		}))

	require.NoError(t, m.Publish(context.Background(), "events", "test.T", nil))

	select {
	case <-received:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message not delivered within 500ms")
	}

	_, err = os.Stat(filepath.Join(overrideDir, "channels", "events"))
	assert.NoError(t, err, "channel dir should exist under the override data dir")
}
