package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Compactor tracks registered subscribers for a single channel and deletes
// fully-consumed sealed segment files. Unlike the old copy-based approach,
// compaction is a simple O(1)-per-segment file deletion with no writer pause.
type Compactor struct {
	mu          sync.Mutex
	offsetDir   string
	subscribers map[string]struct{}
	maxLagBytes int64
	log         logger
}

// NewCompactor returns a Compactor that reads subscriber offsets from offsetDir.
// maxLagBytes is the per-subscriber lag threshold above which a warning is
// logged; 0 disables lag warnings. log may be nil.
func NewCompactor(offsetDir string, maxLagBytes int64, log logger) *Compactor {
	if log == nil {
		log = nopLogger{}
	}
	return &Compactor{
		offsetDir:   offsetDir,
		subscribers: make(map[string]struct{}),
		maxLagBytes: maxLagBytes,
		log:         log,
	}
}

// RegisterSubscriber marks id as a subscriber whose offset constrains deletion.
func (c *Compactor) RegisterSubscriber(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subscribers[id] = struct{}{}
}

// DeregisterSubscriber removes id and deletes its offset file. After removal,
// compaction is no longer constrained by this subscriber's position.
func (c *Compactor) DeregisterSubscriber(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscribers, id)
	offsetPath := filepath.Join(c.offsetDir, id+".offset")
	if err := os.Remove(offsetPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove offset file for subscriber %q: %w", id, err)
	}
	return nil
}

// MinOffset returns the smallest persisted offset across all registered
// subscribers. Returns 0 if no subscribers are registered.
func (c *Compactor) MinOffset() (int64, error) {
	c.mu.Lock()
	ids := make([]string, 0, len(c.subscribers))
	for id := range c.subscribers {
		ids = append(ids, id)
	}
	c.mu.Unlock()

	if len(ids) == 0 {
		return 0, nil
	}

	var minOffset int64 = math.MaxInt64
	for _, id := range ids {
		off, err := ReadOffset(filepath.Join(c.offsetDir, id+".offset"))
		if err != nil {
			return 0, fmt.Errorf("read offset for subscriber %q: %w", id, err)
		}
		if off < minOffset {
			minOffset = off
		}
	}

	// Include federation peer offsets (fed-*.offset).
	if fedMin, err := c.fedMinOffset(); err == nil && fedMin < minOffset {
		minOffset = fedMin
	}

	return minOffset, nil
}

// fedMinOffset returns the minimum byte offset across all fed-*.offset files
// in c.offsetDir. Returns (math.MaxInt64, nil) when none are present, and
// (0, err) on a read error that prevents a safe calculation.
func (c *Compactor) fedMinOffset() (int64, error) {
	entries, err := os.ReadDir(c.offsetDir)
	if os.IsNotExist(err) {
		return math.MaxInt64, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read offset dir for fed offsets: %w", err)
	}
	min := int64(math.MaxInt64)
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "fed-") || !strings.HasSuffix(e.Name(), ".offset") {
			continue
		}
		off, err := ReadOffset(filepath.Join(c.offsetDir, e.Name()))
		if err != nil {
			return 0, fmt.Errorf("read fed offset %q: %w", e.Name(), err)
		}
		if off < min {
			min = off
		}
	}
	return min, nil
}

// MaybeCompact deletes any sealed segment files in channelDir whose contents
// have been fully consumed by all registered subscribers. The active (newest)
// segment is never deleted. No writer pause is needed — the writer only ever
// appends to the active segment, and Unix allows open-fd reads of deleted files
// to complete normally.
//
// Lag warnings are logged for any subscriber whose unconsumed bytes exceed
// maxLagBytes (if configured).
func (c *Compactor) MaybeCompact(channelDir string) error {
	segs, err := listSegments(channelDir)
	if err != nil {
		return fmt.Errorf("list segments: %w", err)
	}
	if len(segs) == 0 {
		return nil
	}

	// Read subscriber offsets once.
	c.mu.Lock()
	ids := make([]string, 0, len(c.subscribers))
	for id := range c.subscribers {
		ids = append(ids, id)
	}
	c.mu.Unlock()

	if len(ids) == 0 {
		return nil
	}

	offsets := make(map[string]int64, len(ids))
	var minOffset int64 = math.MaxInt64
	for _, id := range ids {
		off, err := ReadOffset(filepath.Join(c.offsetDir, id+".offset"))
		if err != nil {
			return fmt.Errorf("read offset for subscriber %q: %w", id, err)
		}
		offsets[id] = off
		if off < minOffset {
			minOffset = off
		}
	}

	// Also include federation peer offset files (fed-*.offset) in the minimum.
	// These are written by channelReader goroutines and must be respected to
	// prevent compacting data that federation peers have not yet consumed.
	if fedMin, err := c.fedMinOffset(); err == nil && fedMin < minOffset {
		minOffset = fedMin
	}

	// Lag warnings: compute total stream size from last segment's end.
	if c.maxLagBytes > 0 {
		active := segs[len(segs)-1]
		streamEnd := active.startOffset + active.size
		for id, off := range offsets {
			if lag := streamEnd - off; lag > c.maxLagBytes {
				c.log.Warn("subscriber is lagging behind channel file",
					"subscriber", id, "lag_bytes", lag, "max_lag_bytes", c.maxLagBytes)
			}
		}
	}

	// Nothing to delete if there is only the active segment.
	if len(segs) <= 1 {
		return nil
	}

	// Delete any sealed segment (not the last) whose entire content lies
	// before minOffset. A segment is fully consumed when the next segment's
	// start offset is <= minOffset.
	for i := 0; i < len(segs)-1; i++ {
		nextStart := segs[i+1].startOffset
		if nextStart <= minOffset {
			if err := os.Remove(segs[i].path); err != nil {
				c.log.Error("delete segment", "path", segs[i].path, "err", err)
			} else {
				c.log.Warn("deleted consumed segment",
					"path", segs[i].path, "bytes_freed", segs[i].size)
			}
		}
	}

	return nil
}
