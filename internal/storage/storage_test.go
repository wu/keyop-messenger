package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
)

func TestChannelStreamEnd_NonexistentDirectory(t *testing.T) {
	end, err := ChannelStreamEnd(filepath.Join(t.TempDir(), "no-such-channel"))
	require.NoError(t, err)
	assert.Zero(t, end)
}

func TestChannelStreamEnd_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "ch")
	require.NoError(t, os.MkdirAll(channelDir, 0o750))

	end, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	assert.Zero(t, end)
}

func TestChannelStreamEnd_AfterWrite(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "ch")

	env, err := envelope.NewEnvelope("ch", "origin", "test.T", map[string]any{"v": 1})
	require.NoError(t, err)
	writeTestEnvelope(t, channelDir, env)

	end, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)
	assert.Positive(t, end)
}

func TestChannelStreamEnd_GrowsWithMoreWrites(t *testing.T) {
	dir := t.TempDir()
	channelDir := filepath.Join(dir, "ch")

	env1, _ := envelope.NewEnvelope("ch", "origin", "test.T", map[string]any{"v": 1})
	writeTestEnvelope(t, channelDir, env1)
	end1, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)

	env2, _ := envelope.NewEnvelope("ch", "origin", "test.T", map[string]any{"v": 2})
	writeTestEnvelope(t, channelDir, env2)
	end2, err := ChannelStreamEnd(channelDir)
	require.NoError(t, err)

	assert.Greater(t, end2, end1)
}
