package messenger

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStats_Empty(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	s := m.Stats()
	assert.Empty(t, s.Channels)
	assert.Empty(t, s.Federation.Clients)
}

func TestStats_ChannelStreamBytes(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Publish(context.Background(), "orders", "test.E", map[string]any{"v": 1}))

	s := m.Stats()
	require.Len(t, s.Channels, 1)
	assert.Equal(t, "orders", s.Channels[0].Channel)
	assert.Positive(t, s.Channels[0].StreamBytes)
	assert.Empty(t, s.Channels[0].Subscribers)
}

func TestStats_SubscriberLagAfterDelivery(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	delivered := make(chan struct{}, 10)
	require.NoError(t, m.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, _ Message) error {
			delivered <- struct{}{}
			return nil
		}))

	require.NoError(t, m.Publish(context.Background(), "events", "test.E", map[string]any{"v": 1}))

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("message not delivered within 2s")
	}

	s := m.Stats()
	var ch *ChannelStats
	for i := range s.Channels {
		if s.Channels[i].Channel == "events" {
			ch = &s.Channels[i]
		}
	}
	require.NotNil(t, ch, "events channel not found in stats")
	assert.Positive(t, ch.StreamBytes)
	require.Len(t, ch.Subscribers, 1)
	assert.Equal(t, "sub1", ch.Subscribers[0].ID)
	assert.Zero(t, ch.Subscribers[0].LagBytes)
}

func TestStats_MultipleChannels(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Publish(context.Background(), "alpha", "test.E", map[string]any{"v": 1}))
	require.NoError(t, m.Publish(context.Background(), "beta", "test.E", map[string]any{"v": 2}))

	s := m.Stats()
	assert.Len(t, s.Channels, 2)

	found := make(map[string]bool)
	for _, ch := range s.Channels {
		assert.Positive(t, ch.StreamBytes)
		found[ch.Channel] = true
	}
	assert.True(t, found["alpha"])
	assert.True(t, found["beta"])
}

func TestStats_StreamBytesGrowsWithPublish(t *testing.T) {
	dir := t.TempDir()
	m, err := New(testConfig(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Publish(context.Background(), "ch", "test.E", map[string]any{"v": 1}))
	s1 := m.Stats()
	require.Len(t, s1.Channels, 1)
	bytesAfterFirst := s1.Channels[0].StreamBytes

	require.NoError(t, m.Publish(context.Background(), "ch", "test.E", map[string]any{"v": 2}))
	s2 := m.Stats()
	require.Len(t, s2.Channels, 1)

	assert.Greater(t, s2.Channels[0].StreamBytes, bytesAfterFirst)
}
