package latencyhist

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketIndex(t *testing.T) {
	// Exact bound values land in their own bucket (bound is the inclusive upper).
	assert.Equal(t, 0, bucketIndex(10_000))                          // == first bound (10µs)
	assert.Equal(t, 0, bucketIndex(1))                               // below first bound
	assert.Equal(t, 1, bucketIndex(10_001))                          // just over 10µs → 25µs bucket
	assert.Equal(t, len(BoundsNanos)-1, bucketIndex(25_000_000_000)) // == last bound (25s)
	assert.Equal(t, len(BoundsNanos), bucketIndex(60_000_000_000))   // > last bound → overflow
}

func TestWindow_ObserveAndLiveBuckets(t *testing.T) {
	w := NewWindow()
	now := time.Now().UnixNano()
	w.nowFn = func() int64 { return now }

	w.Observe(5 * time.Millisecond) // bucket for 5ms
	w.Observe(5 * time.Millisecond)
	w.Observe(2 * time.Second) // bucket for 2.5s

	live := w.LiveBuckets()
	require.Len(t, live, numBuckets)

	var total int64
	for _, c := range live {
		total += c
	}
	assert.Equal(t, int64(3), total)
	assert.Equal(t, int64(2), live[bucketIndex(int64(5*time.Millisecond))])
	assert.Equal(t, int64(1), live[bucketIndex(int64(2*time.Second))])
}

func TestWindow_SlotExpiry(t *testing.T) {
	w := NewWindow()
	base := int64(0)
	w.nowFn = func() int64 { return base }

	w.Observe(1 * time.Millisecond)
	require.Equal(t, int64(1), sum(w.LiveBuckets()), "sample visible immediately")

	// Advance time past the whole window; the old slot falls out.
	base = WindowSlots * SlotNanos
	assert.Equal(t, int64(0), sum(w.LiveBuckets()), "sample should age out of the window")

	// A fresh sample at the new time is visible again.
	w.Observe(1 * time.Millisecond)
	assert.Equal(t, int64(1), sum(w.LiveBuckets()))
}

func TestWindow_PartialExpiry(t *testing.T) {
	w := NewWindow()
	base := int64(0)
	w.nowFn = func() int64 { return base }

	w.Observe(1 * time.Millisecond) // slot at epoch 0

	// Advance one slot and add another sample; both still within the window.
	base = SlotNanos
	w.Observe(1 * time.Millisecond) // slot at epoch 1
	assert.Equal(t, int64(2), sum(w.LiveBuckets()))

	// Advance until epoch 0 falls out but epoch 1 remains.
	base = (WindowSlots) * SlotNanos // epoch == WindowSlots; window covers [1, WindowSlots]
	assert.Equal(t, int64(1), sum(w.LiveBuckets()), "only the older slot should expire")
}

func TestQuantiles_Interpolation(t *testing.T) {
	// 100 samples uniformly in the 1ms bucket (500µs, 1ms]. With linear
	// interpolation the median should sit near the bucket midpoint.
	buckets := make([]int64, numBuckets)
	idx := bucketIndex(int64(1 * time.Millisecond)) // upper bound 1ms, lower 500µs
	buckets[idx] = 100

	qs := Quantiles(buckets, []float64{0.5, 0.9, 0.99})
	require.Len(t, qs, 3)
	for _, q := range qs {
		assert.GreaterOrEqual(t, q, int64(500_000), "within bucket lower bound")
		assert.LessOrEqual(t, q, int64(1_000_000), "within bucket upper bound")
	}
	// p90 should be higher in the bucket than p50.
	assert.Greater(t, qs[1], qs[0])
}

func TestQuantiles_Empty(t *testing.T) {
	qs := Quantiles(make([]int64, numBuckets), []float64{0.5, 0.9, 0.99})
	assert.Equal(t, []int64{0, 0, 0}, qs)
}

func TestQuantiles_OverflowSaturates(t *testing.T) {
	buckets := make([]int64, numBuckets)
	buckets[numBuckets-1] = 10 // all samples in the overflow bucket
	qs := Quantiles(buckets, []float64{0.99})
	assert.Equal(t, BoundsNanos[len(BoundsNanos)-1], qs[0], "overflow saturates at the largest bound")
}

func TestQuantiles_WrongLength(t *testing.T) {
	qs := Quantiles([]int64{1, 2, 3}, []float64{0.5})
	assert.Equal(t, []int64{0}, qs)
}

func sum(b []int64) int64 {
	var t int64
	for _, c := range b {
		t += c
	}
	return t
}
