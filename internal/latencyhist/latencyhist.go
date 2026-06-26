// Package latencyhist provides a fixed-bucket latency histogram with a trailing
// sliding time window, used to estimate recent p50/p90/p99 latency for Stats().
//
// The bucket boundaries are an internal implementation detail: callers observe
// durations and read back percentile estimates, never the raw buckets, so the
// public Stats API carries no bucket assumptions. Percentiles are estimated over
// an approximately 60-second trailing window (WindowSlots slots of SlotNanos
// each), so they reflect recent behaviour rather than the whole process lifetime.
package latencyhist

import (
	"sort"
	"sync"
	"time"
)

// BoundsNanos are the inclusive upper bounds of the histogram buckets, in
// nanoseconds, on a 1-2.5-5-per-decade scale from 10µs to 25s. A sample of
// duration d lands in the first bucket whose bound is >= d; samples larger than
// the last bound land in a final overflow bucket. The set is unexported in
// spirit — it is not part of the messenger's public Stats contract — but is
// package-exported so tests can reference it.
var BoundsNanos = [...]int64{
	10_000, 25_000, 50_000, 100_000, 250_000, 500_000, // 10µs … 500µs
	1_000_000, 2_500_000, 5_000_000, 10_000_000, 25_000_000, 50_000_000, // 1ms … 50ms
	100_000_000, 250_000_000, 500_000_000, // 100ms … 500ms
	1_000_000_000, 2_500_000_000, 5_000_000_000, 10_000_000_000, 25_000_000_000, // 1s … 25s
}

// numBuckets is the bound count plus one overflow bucket.
const numBuckets = len(BoundsNanos) + 1

// Window sizing: WindowSlots slots of SlotNanos each form the trailing window.
// 6 × 10s ≈ 60s. The window ages out in SlotNanos steps, so at any instant it
// covers between (WindowSlots-1)*SlotNanos and WindowSlots*SlotNanos of history.
const (
	WindowSlots = 6
	SlotNanos   = int64(10 * time.Second)
)

// bucketIndex returns the bucket a non-negative nanosecond duration falls into:
// the first bound >= ns, or the overflow bucket (len(BoundsNanos)) when ns
// exceeds every bound.
func bucketIndex(ns int64) int {
	return sort.Search(len(BoundsNanos), func(i int) bool { return ns <= BoundsNanos[i] })
}

// Window is a concurrency-safe sliding-window latency histogram. The zero value
// is not usable; construct with NewWindow.
type Window struct {
	mu    sync.Mutex
	slots [WindowSlots]windowSlot
	// nowFn returns the current time in Unix nanoseconds. Overridable in tests.
	nowFn func() int64
}

// windowSlot holds one time slot's bucket counts. epoch identifies which slot of
// time the counts belong to (Unix-nanos / SlotNanos); a stale epoch means the
// slot is reused for a newer time and its counts are cleared on first write.
type windowSlot struct {
	epoch  int64
	counts [numBuckets]int64
}

// NewWindow constructs an empty sliding-window histogram.
func NewWindow() *Window {
	return &Window{nowFn: func() int64 { return time.Now().UnixNano() }}
}

// Observe records one latency sample. Negative durations are clamped to zero.
func (w *Window) Observe(d time.Duration) {
	ns := int64(d)
	if ns < 0 {
		ns = 0
	}
	epoch := w.nowFn() / SlotNanos
	bi := bucketIndex(ns)

	w.mu.Lock()
	s := &w.slots[epoch%WindowSlots]
	if s.epoch != epoch {
		// This ring position now represents a newer time slot; reset it.
		s.epoch = epoch
		s.counts = [numBuckets]int64{}
	}
	s.counts[bi]++
	w.mu.Unlock()
}

// LiveBuckets returns the per-bucket sample counts summed across the slots that
// fall within the current trailing window. The returned slice has length
// numBuckets and is owned by the caller, so it can be merged element-wise with
// other windows (e.g. summing across subscribers) before computing percentiles.
func (w *Window) LiveBuckets() []int64 {
	epoch := w.nowFn() / SlotNanos
	minEpoch := epoch - (WindowSlots - 1)

	out := make([]int64, numBuckets)
	w.mu.Lock()
	for i := range w.slots {
		s := &w.slots[i]
		if s.epoch >= minEpoch && s.epoch <= epoch {
			for b := 0; b < numBuckets; b++ {
				out[b] += s.counts[b]
			}
		}
	}
	w.mu.Unlock()
	return out
}

// Quantiles estimates the requested quantiles (each in [0,1]) from a bucket-count
// slice of length numBuckets, returning nanosecond estimates in the same order.
// It linearly interpolates within the containing bucket assuming a uniform
// distribution, matching the usual fixed-bucket-histogram approach. An estimate
// that falls in the overflow bucket saturates at the largest bound. When the
// buckets are empty (or the slice is the wrong length) every result is 0.
func Quantiles(buckets []int64, qs []float64) []int64 {
	out := make([]int64, len(qs))
	if len(buckets) != numBuckets {
		return out
	}
	var total int64
	for _, c := range buckets {
		total += c
	}
	if total == 0 {
		return out
	}
	for k, q := range qs {
		out[k] = quantile(buckets, total, q)
	}
	return out
}

// quantile estimates a single quantile from non-empty buckets with known total.
func quantile(buckets []int64, total int64, q float64) int64 {
	target := q * float64(total)
	var cum int64
	for i, c := range buckets {
		if c == 0 {
			continue
		}
		cumBefore := cum
		cum += c
		if float64(cum) < target {
			continue
		}
		if i >= len(BoundsNanos) {
			// Overflow bucket: no upper bound, saturate at the largest bound.
			return BoundsNanos[len(BoundsNanos)-1]
		}
		lower := int64(0)
		if i > 0 {
			lower = BoundsNanos[i-1]
		}
		upper := BoundsNanos[i]
		frac := (target - float64(cumBefore)) / float64(c)
		return lower + int64(frac*float64(upper-lower))
	}
	// Should be unreachable for total > 0; fall back to the largest bound.
	return BoundsNanos[len(BoundsNanos)-1]
}
