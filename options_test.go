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

	m, err := New(nil, WithConfig(cfg), WithTestIdentity("via-option"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, "via-option", m.InstanceName())
}

// TestWithConfigPositionalTakesPrecedence verifies that when both the
// positional Config and WithConfig are provided, the positional arg wins.
// (Identity is supplied independently via WithTestIdentity; the precedence
// being tested here is the Config object, not the identity.)
func TestWithConfigPositionalTakesPrecedence(t *testing.T) {
	dir := t.TempDir()
	positional := testConfig(dir)
	optionCfg := testConfig(t.TempDir()) // distinct dir to detect which won

	m, err := New(positional, WithConfig(optionCfg), WithTestIdentity("positional"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	assert.Equal(t, "positional", m.InstanceName())
	assert.Equal(t, dir, m.cfg.Storage.DataDir, "positional config must win over WithConfig")
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
	m, err := New(cfg, WithDataDir(overrideDir), WithTestIdentity("test-instance"))
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

	m, err := New(nil, WithConfig(cfg), WithDataDir(overrideDir), WithTestIdentity("combo"))
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

// TestWithMaxRetries_SetsOption verifies WithMaxRetries records an override,
// including an explicit 0 (distinguished from "not set" by the pointer).
func TestWithMaxRetries_SetsOption(t *testing.T) {
	var so subscribeOptions
	assert.Nil(t, so.maxRetries, "unset by default")

	WithMaxRetries(0)(&so)
	require.NotNil(t, so.maxRetries)
	assert.Equal(t, 0, *so.maxRetries)

	WithMaxRetries(9)(&so)
	require.NotNil(t, so.maxRetries)
	assert.Equal(t, 9, *so.maxRetries)
}

// TestWithRetryBackoff_SetsOption verifies WithRetryBackoff records the schedule.
func TestWithRetryBackoff_SetsOption(t *testing.T) {
	var so subscribeOptions
	assert.False(t, so.retryBackoffSet, "unset by default")

	WithRetryBackoff(250*time.Millisecond, 10*time.Second)(&so)
	assert.True(t, so.retryBackoffSet)
	assert.Equal(t, 250*time.Millisecond, so.retryBase)
	assert.Equal(t, 10*time.Second, so.retryMax)
}
