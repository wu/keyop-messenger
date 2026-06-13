//go:build integration

package federation

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/storage"
)

// channelFeeder is a test helper that writes envelopes to a Client's local
// channel directory and wakes the Client's outbound reader. It replaces the
// pre-disk-pull pattern where tests called client.Sender().Enqueue(env).
//
// The writer uses storage.NewChannelWriter so the envelope is written to a
// real segment file at the path the client's channelReader is watching.
type channelFeeder struct {
	writer  storage.ChannelWriter
	client  *Client
	channel string
}

// newChannelFeeder opens a channelWriter for dataDir/channels/channel and
// returns a feeder bound to client.NotifyChannel. The writer is closed on
// test cleanup.
func newChannelFeeder(t *testing.T, dataDir, channel string, client *Client) *channelFeeder {
	t.Helper()
	channelDir := filepath.Join(dataDir, "channels", channel)
	w, err := storage.NewChannelWriter(channelDir, 0, 0, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return &channelFeeder{writer: w, client: client, channel: channel}
}

// publish writes env to the channel file and wakes the client reader.
func (f *channelFeeder) publish(t *testing.T, env *envelope.Envelope) {
	t.Helper()
	require.NoError(t, f.writer.Write(context.Background(), env))
	f.client.NotifyChannel(f.channel)
}
