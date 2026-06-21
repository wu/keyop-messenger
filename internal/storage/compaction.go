package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Compactor tracks registered subscribers for a single channel and deletes
// fully-consumed sealed segment files. Unlike the old copy-based approach,
// compaction is a simple O(1)-per-segment file deletion with no writer pause.
//
// In addition to consumption-based deletion, the compactor enforces optional
// retention bounds (maxSizeBytes, retention age). When a channel exceeds a
// bound, the oldest sealed segments are force-deleted even if a subscriber has
// not consumed them — trading bounded disk usage for dropped messages on
// lagging subscribers. The active (newest) segment is never deleted.
type Compactor struct {
	mu           sync.Mutex
	offsetDir    string
	subscribers  map[string]struct{}
	maxSizeBytes int64         // 0 = no size cap
	retention    time.Duration // 0 = no age cap
	now          func() time.Time
	log          logger
}

// NewCompactor returns a Compactor that reads subscriber offsets from offsetDir.
//
//   - maxSizeBytes: hard cap on a channel's retained on-disk bytes; when exceeded,
//     oldest sealed segments are force-deleted regardless of consumption. 0 disables.
//   - retention: max age of retained messages; sealed segments older than this are
//     force-deleted regardless of consumption. 0 disables.
//
// log may be nil.
func NewCompactor(offsetDir string, maxSizeBytes int64, retention time.Duration, log logger) *Compactor {
	if log == nil {
		log = nopLogger{}
	}
	return &Compactor{
		offsetDir:    offsetDir,
		subscribers:  make(map[string]struct{}),
		maxSizeBytes: maxSizeBytes,
		retention:    retention,
		now:          time.Now,
		log:          log,
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

// fedMinOffset returns the minimum byte offset across all federation offset
// files in c.offsetDir. This covers both hub-side inbound peer offsets
// ("fed-*.offset", written by hub channelReaders) and client-side outbound
// publish offsets ("fedout-*.offset", written by client channelReaders).
// Returns (math.MaxInt64, nil) when none are present, and (0, err) on a read
// error that prevents a safe calculation.
func (c *Compactor) fedMinOffset() (int64, error) {
	entries, err := os.ReadDir(c.offsetDir)
	if os.IsNotExist(err) {
		return math.MaxInt64, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read offset dir for fed offsets: %w", err)
	}
	minOffset := int64(math.MaxInt64)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".offset") {
			continue
		}
		if !strings.HasPrefix(e.Name(), "fed-") && !strings.HasPrefix(e.Name(), "fedout-") {
			continue
		}
		off, err := ReadOffset(filepath.Join(c.offsetDir, e.Name()))
		if err != nil {
			return 0, fmt.Errorf("read fed offset %q: %w", e.Name(), err)
		}
		if off < minOffset {
			minOffset = off
		}
	}
	return minOffset, nil
}

// MaybeCompact deletes any sealed segment files in channelDir whose contents
// have been fully consumed by all registered subscribers. The active (newest)
// segment is never deleted. No writer pause is needed — the writer only ever
// appends to the active segment, and Unix allows open-fd reads of deleted files
// to complete normally.
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

	var minOffset int64 = math.MaxInt64
	for _, id := range ids {
		off, err := ReadOffset(filepath.Join(c.offsetDir, id+".offset"))
		if err != nil {
			return fmt.Errorf("read offset for subscriber %q: %w", id, err)
		}
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

	// Nothing to delete if there is only the active segment.
	if len(segs) <= 1 {
		return nil
	}

	// Total retained bytes across all segments, used for the size cap. It is
	// decremented as segments are deleted so the cap is re-evaluated each step.
	var total int64
	for _, s := range segs {
		total += s.size
	}
	now := c.now()

	// Walk sealed segments oldest-first (never the active/last segment). Delete a
	// segment when it is either fully consumed, or — when a retention bound is
	// configured and exceeded — force it out even if unconsumed. A segment that is
	// consumed, within the size cap, and within the age bound is retained; because
	// consumption, size, and age are all monotonic oldest-first, every newer sealed
	// segment is then also retained, so we can stop.
	for i := 0; i < len(segs)-1; i++ {
		seg := segs[i]
		nextStart := segs[i+1].startOffset

		consumed := nextStart <= minOffset
		overSize := c.maxSizeBytes > 0 && total > c.maxSizeBytes
		tooOld := c.retention > 0 && now.Sub(seg.modTime) > c.retention

		if !consumed && !overSize && !tooOld {
			break
		}

		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			c.log.Error("delete segment", "path", seg.path, "err", err)
			break // stop on a real failure to avoid spinning over an undeletable file
		}
		total -= seg.size

		switch {
		case consumed:
			c.log.Warn("deleted consumed segment",
				"path", seg.path, "bytes_freed", seg.size)
		case overSize:
			c.log.Warn("force-deleted unconsumed segment to enforce size cap",
				"path", seg.path, "bytes_freed", seg.size,
				"next_offset", nextStart, "min_subscriber_offset", minOffset,
				"max_size_bytes", c.maxSizeBytes)
		case tooOld:
			c.log.Warn("force-deleted unconsumed segment past retention age",
				"path", seg.path, "bytes_freed", seg.size,
				"age", now.Sub(seg.modTime).String(), "retention", c.retention.String())
		}
	}

	return nil
}
