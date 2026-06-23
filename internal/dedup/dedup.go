// Package dedup provides deduplication of envelope IDs.
package dedup

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

// Deduplicator is the seen-ID set.
type Deduplicator interface {
	SeenOrAdd(id string) bool // returns true if already seen
}

// LRUDedup is a thread-safe, fixed-capacity LRU set of message IDs.
type LRUDedup struct {
	cache *lru.Cache[string, struct{}]
}

// NewLRUDedup returns an LRUDedup with the given capacity.
func NewLRUDedup(size int) (*LRUDedup, error) {
	c, err := lru.New[string, struct{}](size)
	if err != nil {
		return nil, err
	}
	return &LRUDedup{cache: c}, nil
}

// SeenOrAdd returns true if id was already in the set, false if it was newly
// added. The operation is atomic with respect to concurrent callers.
func (d *LRUDedup) SeenOrAdd(id string) bool {
	ok, _ := d.cache.ContainsOrAdd(id, struct{}{})
	return ok
}

// Remove deletes id from the set. It rolls back a speculative SeenOrAdd when the
// work that mark guarded (e.g. a durable batch commit) fails, so the delivery,
// once retried, is not suppressed as a duplicate. Marking with SeenOrAdd before
// the commit still atomically suppresses concurrent dual-path arrivals of the
// same ID; only the failing path un-marks.
func (d *LRUDedup) Remove(id string) {
	_ = d.cache.Remove(id)
}
