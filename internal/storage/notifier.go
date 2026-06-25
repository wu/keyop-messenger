package storage

import (
	"sync/atomic"
)

// LocalNotifier provides an in-process notification channel that wakes a
// same-process subscriber when new data is written to its channel. Use
// LocalNotifier.Notify as the notifyFn argument to NewChannelWriter; have the
// subscriber select on LocalNotifier.C().
type LocalNotifier struct {
	ch       chan struct{}
	notifies atomic.Int64
	drops    atomic.Int64
}

// NewLocalNotifier creates a LocalNotifier with a capacity-1 channel.
func NewLocalNotifier() *LocalNotifier {
	return &LocalNotifier{ch: make(chan struct{}, 1)}
}

// Notify sends a non-blocking notification. Safe to call from any goroutine.
func (n *LocalNotifier) Notify() {
	select {
	case n.ch <- struct{}{}:
		n.notifies.Add(1)
	default:
		n.drops.Add(1)
	}
}

// C returns the read-only channel that subscribers should select on.
func (n *LocalNotifier) C() <-chan struct{} { return n.ch }

// Stats returns total successful notifies and dropped notifies since creation.
// Intended for diagnostics and tests investigating lost-wakeup scenarios.
func (n *LocalNotifier) Stats() (notifies, drops int64) {
	return n.notifies.Load(), n.drops.Load()
}
