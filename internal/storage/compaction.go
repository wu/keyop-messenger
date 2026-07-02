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
// retention bounds (max files, retention age). When a channel exceeds a
// bound, the oldest sealed segments are force-deleted even if a subscriber has
// not consumed them — trading bounded disk usage for dropped messages on
// lagging subscribers. The active (newest) segment is never deleted.
type Compactor struct {
	mu          sync.Mutex
	offsetDir   string
	subscribers map[string]struct{}
	maxFiles    int           // 0 = no file cap; >0 = max total segment files (incl. active) retained
	retention   time.Duration // 0 = no age cap
	now         func() time.Time
	log         logger
}

// NewCompactor returns a Compactor that reads subscriber offsets from offsetDir.
//
//   - maxFiles: max number of segment files retained per channel, counting the
//     active segment; when the total exceeds it, the oldest sealed segments are
//     force-deleted regardless of consumption. The active segment is never
//     deleted, so an effective floor of 1 always applies. 0 disables.
//   - retention: max age of retained messages; sealed segments older than this are
//     force-deleted regardless of consumption. 0 disables.
//
// log may be nil.
func NewCompactor(offsetDir string, maxFiles int, retention time.Duration, log logger) *Compactor {
	if log == nil {
		log = nopLogger{}
	}
	return &Compactor{
		offsetDir:   offsetDir,
		subscribers: make(map[string]struct{}),
		maxFiles:    maxFiles,
		retention:   retention,
		now:         time.Now,
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

	// Include every persisted offset file on disk (see persistedMinOffset).
	if diskMin, err := c.persistedMinOffset(); err == nil && diskMin < minOffset {
		minOffset = diskMin
	}

	return minOffset, nil
}

// persistedMinOffset returns the minimum byte offset across every *.offset file
// in c.offsetDir. This deliberately covers all offset-file kinds:
//
//   - plain subscriber offsets ("{subscriber-id}.offset", written by the
//     subscriber goroutine)
//   - hub-side inbound federation peer offsets ("fed-*.offset")
//   - client-side outbound federation publish offsets ("fedout-*.offset")
//
// Scanning by file — rather than by the in-memory registered-subscriber set —
// is what makes compaction safe across a process restart: a subscriber's offset
// file persists on disk even before that subscriber re-subscribes in the new
// process, so its unconsumed position still constrains deletion during the
// restart window. Restricting the scan to registered subscribers would leave a
// hole where a not-yet-re-subscribed consumer's sealed segments are deleted out
// from under it (silent at-least-once data loss).
//
// Returns (math.MaxInt64, nil) when no offset files are present, and (0, err) on
// a read error that prevents a safe calculation. Callers treat a non-nil error
// as "cannot lower the minimum from disk" and skip the result rather than
// aborting compaction.
func (c *Compactor) persistedMinOffset() (int64, error) {
	entries, err := os.ReadDir(c.offsetDir)
	if os.IsNotExist(err) {
		return math.MaxInt64, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read offset dir: %w", err)
	}
	minOffset := int64(math.MaxInt64)
	for _, e := range entries {
		// The ".offset" suffix check excludes in-flight ".offset.tmp" files
		// written by WriteOffset's atomic rename.
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".offset") {
			continue
		}
		off, err := ReadOffset(filepath.Join(c.offsetDir, e.Name()))
		if err != nil {
			return 0, fmt.Errorf("read offset %q: %w", e.Name(), err)
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

	// Also fold in every persisted offset file on disk. Beyond the in-memory
	// registered subscribers above (whose files this re-reads harmlessly), this
	// captures federation peer offsets and — critically — plain subscriber
	// offset files whose owner has not re-subscribed yet this process lifetime.
	// Without it, a subscriber that is slow to initialize after a restart would
	// have its unconsumed sealed segments deleted (silent data loss). A read
	// error (e.g. a corrupt fed offset) is ignored so it cannot block deletion.
	if diskMin, err := c.persistedMinOffset(); err == nil && diskMin < minOffset {
		minOffset = diskMin
	}

	// Nothing to delete if there is only the active segment.
	if len(segs) <= 1 {
		return nil
	}

	// Total segment files including the active/last one. The file cap force-evicts
	// the oldest sealed segments until the total is within maxFiles, even if
	// unconsumed; decremented as segments are deleted so the cap re-evaluates each
	// step. The active segment is never deleted, so the effective floor is 1.
	total := len(segs)
	now := c.now()

	// Walk sealed segments oldest-first (never the active/last segment). Delete a
	// segment when it is either fully consumed, or — when a retention bound is
	// configured and exceeded — force it out even if unconsumed. A segment that is
	// consumed, within the file cap, and within the age bound is retained; because
	// consumption, file count, and age are all monotonic oldest-first, every newer
	// sealed segment is then also retained, so we can stop.
	for i := 0; i < len(segs)-1; i++ {
		seg := segs[i]
		nextStart := segs[i+1].startOffset

		consumed := nextStart <= minOffset
		overCount := c.maxFiles > 0 && total > c.maxFiles
		tooOld := c.retention > 0 && now.Sub(seg.modTime) > c.retention

		if !consumed && !overCount && !tooOld {
			break
		}

		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			c.log.Error("delete segment", "path", seg.path, "err", err)
			break // stop on a real failure to avoid spinning over an undeletable file
		}
		total--

		switch {
		case consumed:
			c.log.Warn("deleted consumed segment",
				"path", seg.path, "bytes_freed", seg.size)
		case overCount:
			c.log.Warn("force-deleted unconsumed segment to enforce max-files cap",
				"path", seg.path, "bytes_freed", seg.size,
				"next_offset", nextStart, "min_subscriber_offset", minOffset,
				"max_files", c.maxFiles)
		case tooOld:
			c.log.Warn("force-deleted unconsumed segment past retention age",
				"path", seg.path, "bytes_freed", seg.size,
				"age", now.Sub(seg.modTime).String(), "retention", c.retention.String())
		}
	}

	return nil
}
