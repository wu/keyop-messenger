package messenger

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/envelope"
)

func TestStats_Empty(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	s := m.Stats()
	assert.Empty(t, s.Channels)
	assert.Empty(t, s.Federation.Clients)
	assert.Nil(t, s.Federation.Hub, "local-only instance should not report hub stats")
}

func TestStats_ChannelStreamBytes(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.Publish(context.Background(), "orders", "test.E", map[string]any{"v": 1}))

	s := m.Stats()
	require.Len(t, s.Channels, 1)
	assert.Equal(t, "orders", s.Channels[0].Channel)
	assert.Positive(t, s.Channels[0].StreamBytes)
	// With nothing compacted, the on-disk footprint equals the stream end.
	assert.Equal(t, s.Channels[0].StreamBytes, s.Channels[0].DiskBytes)
	assert.Equal(t, int64(1), s.Channels[0].MessageCount)
	assert.Empty(t, s.Channels[0].Subscribers)
}

func TestStats_SubscriberLagAfterDelivery(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
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

	// The handler signals delivery (delivered<-) before the subscriber advances
	// its offset, so lag can briefly be non-zero. Poll until the offset catches
	// up rather than reading a single racy snapshot.
	require.Eventually(t, func() bool {
		s := m.Stats()
		for _, c := range s.Channels {
			if c.Channel == "events" && len(c.Subscribers) == 1 {
				// A caught-up subscriber has no byte lag and no oldest-pending record.
				return c.Subscribers[0].LagBytes == 0 && c.Subscribers[0].OldestPendingUnixMs == 0
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "subscriber lag should reach zero after delivery")
}

// TestStats_OldestPendingWhileBehind verifies that a subscriber parked on an
// undelivered message reports that message's publish timestamp as the oldest
// pending, so time-based lag is observable even when byte lag is small.
func TestStats_OldestPendingWhileBehind(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// The handler parks on the first message so the subscriber's offset never
	// advances; release is closed in cleanup so Close() does not hang.
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	entered := make(chan struct{}, 1)
	require.NoError(t, m.Subscribe(context.Background(), "events", "sub1",
		func(_ context.Context, _ Message) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return nil
		}))

	before := time.Now().UnixMilli()
	require.NoError(t, m.Publish(context.Background(), "events", "test.E", map[string]any{"v": 1}))

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler not entered within 2s")
	}
	after := time.Now().UnixMilli()

	// While the handler is parked the subscriber stays behind.
	require.Eventually(t, func() bool {
		for _, c := range m.Stats().Channels {
			if c.Channel == "events" && len(c.Subscribers) == 1 {
				return c.Subscribers[0].LagBytes > 0
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "subscriber should be behind while handler is parked")

	var sub *SubscriberStats
	for _, c := range m.Stats().Channels {
		if c.Channel == "events" && len(c.Subscribers) == 1 {
			s := c.Subscribers[0]
			sub = &s
		}
	}
	require.NotNil(t, sub)
	assert.GreaterOrEqual(t, sub.OldestPendingUnixMs, before,
		"oldest pending should be at or after the publish time")
	assert.LessOrEqual(t, sub.OldestPendingUnixMs, after,
		"oldest pending should be at or before the handler entered")
}

func TestStats_MultipleChannels(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
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
	m, err := newForTest(dir)
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
	assert.Equal(t, int64(2), s2.Channels[0].MessageCount)
}

func TestStats_MessageCountIncludesFederated(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	// One locally-published message.
	require.NoError(t, m.Publish(context.Background(), "ch", "test.E", map[string]any{"v": 1}))

	// One message arriving via federation (writeLocalEnvelope is the callback
	// used by PeerReceiver/Hub when a remote message is written locally).
	env, err := envelope.NewEnvelope("ch", "remote-origin", "test.E", map[string]any{"v": 2})
	require.NoError(t, err)
	require.NoError(t, m.writeLocalEnvelope(&env))

	s := m.Stats()
	require.Len(t, s.Channels, 1)
	assert.Equal(t, int64(2), s.Channels[0].MessageCount)
}
