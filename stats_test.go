package messenger

import (
	"context"
	"fmt"
	"strings"
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

// TestStats_Totals verifies the instance-wide aggregates: regular-channel traffic
// and saturation sums, plus the dead-letter error totals reported separately.
func TestStats_Totals(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()

	// Two regular channels, each with a subscriber. "alpha" gets two messages;
	// "beta" gets one. Subscribers consume so lag settles to zero.
	consumed := make(chan struct{}, 16)
	okHandler := func(_ context.Context, _ Message) error {
		consumed <- struct{}{}
		return nil
	}
	require.NoError(t, m.Subscribe(ctx, "alpha", "alpha-sub", okHandler))
	require.NoError(t, m.Publish(ctx, "alpha", "test.E", map[string]any{"v": 1}))
	require.NoError(t, m.Publish(ctx, "alpha", "test.E", map[string]any{"v": 2}))

	// "beta" has a handler that always fails with no retries, so its single
	// message is dead-lettered.
	require.NoError(t, m.Subscribe(ctx, "beta", "beta-sub",
		func(_ context.Context, _ Message) error {
			consumed <- struct{}{}
			return fmt.Errorf("always fail")
		},
		WithMaxRetries(0),
	))
	require.NoError(t, m.Publish(ctx, "beta", "test.E", map[string]any{"v": 3}))

	// Wait until the dead-letter has been written and lag has settled.
	require.Eventually(t, func() bool {
		return m.Stats().Totals.DeadLetterMessages == 1
	}, 2*time.Second, 10*time.Millisecond, "beta message should be dead-lettered")

	var s Stats
	require.Eventually(t, func() bool {
		s = m.Stats()
		return s.Totals.LagBytes == 0
	}, 2*time.Second, 10*time.Millisecond, "subscriber lag should settle to zero")

	tot := s.Totals
	// Two non-dead-letter channels (alpha, beta); the beta.dead-letter channel is
	// excluded from these gauges and rolled into the dead-letter totals.
	assert.Equal(t, 2, tot.Channels, "non-dead-letter channel count")
	assert.Equal(t, 2, tot.Subscribers, "active subscriber count")
	assert.Equal(t, int64(3), tot.MessagesPublished, "alpha(2) + beta(1)")
	assert.Positive(t, tot.StreamBytes)
	assert.Positive(t, tot.DiskBytes)

	// Dead-letter error signal: beta's one failed message.
	assert.Equal(t, int64(1), tot.DeadLetterMessages)
	assert.Positive(t, tot.DeadLetterBytes)

	// The totals must equal the sum of the per-channel stats they aggregate.
	var wantMsgs, wantStream, wantDisk, wantDLMsgs, wantDLBytes int64
	var wantSubs, wantChans int
	for _, ch := range s.Channels {
		if strings.HasSuffix(ch.Channel, ".dead-letter") {
			wantDLMsgs += ch.MessageCount
			wantDLBytes += ch.DiskBytes
			continue
		}
		wantChans++
		wantSubs += len(ch.Subscribers)
		wantMsgs += ch.MessageCount
		wantStream += ch.StreamBytes
		wantDisk += ch.DiskBytes
	}
	assert.Equal(t, wantChans, tot.Channels)
	assert.Equal(t, wantSubs, tot.Subscribers)
	assert.Equal(t, wantMsgs, tot.MessagesPublished)
	assert.Equal(t, wantStream, tot.StreamBytes)
	assert.Equal(t, wantDisk, tot.DiskBytes)
	assert.Equal(t, wantDLMsgs, tot.DeadLetterMessages)
	assert.Equal(t, wantDLBytes, tot.DeadLetterBytes)
}

// TestStats_LatencyAggregates verifies the per-stage latency aggregates: a
// publish records a publish-to-disk sample, and a successful consume records a
// consume-age sample and a handler-latency sample whose summed duration reflects
// the time the handler spent.
func TestStats_LatencyAggregates(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()

	const handlerDelay = 20 * time.Millisecond
	delivered := make(chan struct{}, 4)
	require.NoError(t, m.Subscribe(ctx, "events", "sub",
		func(_ context.Context, _ Message) error {
			time.Sleep(handlerDelay)
			delivered <- struct{}{}
			return nil
		}))

	require.NoError(t, m.Publish(ctx, "events", "test.E", map[string]any{"v": 1}))

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("message not delivered within 2s")
	}

	// The handler signals before dispatch records the sample and advances the
	// offset, so poll until the consume sample lands.
	var s Stats
	require.Eventually(t, func() bool {
		s = m.Stats()
		return s.Latency.Consume.Count == 1
	}, 2*time.Second, 10*time.Millisecond, "consume latency sample should be recorded")

	lat := s.Latency

	// Publish-to-disk: one write op, positive duration.
	assert.Equal(t, int64(1), lat.PublishToDisk.Count)
	assert.Positive(t, lat.PublishToDisk.SumNanos)

	// Handler latency: one sample, at least the artificial handler delay.
	assert.Equal(t, int64(1), lat.Handler.Count)
	assert.GreaterOrEqual(t, lat.Handler.SumNanos, int64(handlerDelay),
		"handler latency should be at least the handler's sleep")

	// Consume age (publish -> handler done) includes the handler time, so it is
	// at least as large as the handler latency.
	assert.Equal(t, int64(1), lat.Consume.Count)
	assert.GreaterOrEqual(t, lat.Consume.SumNanos, lat.Handler.SumNanos,
		"consume age should be >= handler latency")

	// Percentiles over the trailing window are populated for the recorded stages.
	assert.Positive(t, lat.Handler.P50Nanos)
	assert.Positive(t, lat.PublishToDisk.P50Nanos)
	assert.Positive(t, lat.Consume.P50Nanos)
	// The single ~20ms handler sample lands in the (10ms, 25ms] bucket. Within a
	// bucket the estimate is interpolated by the quantile fraction, so p50 < p99
	// but both stay inside the bucket bounds.
	assert.LessOrEqual(t, lat.Handler.P50Nanos, lat.Handler.P99Nanos)
	assert.GreaterOrEqual(t, lat.Handler.P50Nanos, int64(10*time.Millisecond))
	assert.LessOrEqual(t, lat.Handler.P99Nanos, int64(25*time.Millisecond))
}

// TestStats_LatencyPercentilesAcrossSubscribers verifies consume/handler
// percentiles are computed from the histogram merged across all subscribers,
// not by averaging per-subscriber percentiles.
func TestStats_LatencyPercentilesAcrossSubscribers(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	ctx := context.Background()
	delivered := make(chan struct{}, 8)
	for _, id := range []string{"sub-a", "sub-b"} {
		require.NoError(t, m.Subscribe(ctx, "events", id,
			func(_ context.Context, _ Message) error {
				delivered <- struct{}{}
				return nil
			}))
	}

	require.NoError(t, m.Publish(ctx, "events", "test.E", map[string]any{"v": 1}))

	for i := 0; i < 2; i++ {
		select {
		case <-delivered:
		case <-time.After(2 * time.Second):
			t.Fatal("message not delivered to both subscribers")
		}
	}

	var lat Latency
	require.Eventually(t, func() bool {
		lat = m.Stats().Latency
		return lat.Consume.Count == 2
	}, 2*time.Second, 10*time.Millisecond, "both subscribers' consume samples should be recorded")

	// Two samples merged from two subscribers; percentiles are non-zero.
	assert.Equal(t, int64(2), lat.Handler.Count)
	assert.Positive(t, lat.Consume.P90Nanos)
	assert.Positive(t, lat.Handler.P90Nanos)
}

// TestStats_LatencyBatchCountsOnce verifies a PublishBatch records a single
// publish-to-disk sample regardless of how many messages it carries.
func TestStats_LatencyBatchCountsOnce(t *testing.T) {
	dir := t.TempDir()
	m, err := newForTest(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })

	require.NoError(t, m.PublishBatch(context.Background(), "events", []BatchMessage{
		{PayloadType: "test.E", Payload: map[string]any{"v": 1}},
		{PayloadType: "test.E", Payload: map[string]any{"v": 2}},
		{PayloadType: "test.E", Payload: map[string]any{"v": 3}},
	}))

	lat := m.Stats().Latency
	assert.Equal(t, int64(1), lat.PublishToDisk.Count, "a batch is one write operation")
	assert.Positive(t, lat.PublishToDisk.SumNanos)
}
