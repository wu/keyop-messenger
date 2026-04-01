package storage

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

// pauseableWriter is satisfied by *channelWriter. It allows the compactor to
// atomically rename the channel file and reopen the writer's file descriptor
// without any writes occurring during the swap.
type pauseableWriter interface {
	PauseAndSwap(fn func() error) error
}

// Compactor tracks registered subscribers for a single channel and performs
// file compaction once all subscribers have consumed past a configurable
// threshold. It is not safe to share across channels.
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

// RegisterSubscriber marks id as a subscriber whose offset must be considered
// when computing the safe compaction boundary.
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
// subscribers. Returns 0 if no subscribers are registered (conservative: do
// not compact without a known consumer boundary).
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

	var min int64 = math.MaxInt64
	for _, id := range ids {
		off, err := ReadOffset(filepath.Join(c.offsetDir, id+".offset"))
		if err != nil {
			return 0, fmt.Errorf("read offset for subscriber %q: %w", id, err)
		}
		if off < min {
			min = off
		}
	}
	return min, nil
}

// MaybeCompact compacts channelPath if the minimum subscriber offset exceeds
// threshold. When compaction is triggered the writer pw is paused for the
// duration of the file swap (copy → fsync → rename) and all subscriber offsets
// are adjusted by subtracting the removed prefix. Returns nil without
// modification when compaction is not needed.
//
// Lag warnings are emitted for any subscriber whose unconsumed byte count
// exceeds the maxLagBytes configured at construction.
func (c *Compactor) MaybeCompact(channelPath string, threshold int64, pw pauseableWriter) error {
	info, err := os.Stat(channelPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat channel file: %w", err)
	}
	fileSize := info.Size()

	// Snapshot subscriber set and read all offsets.
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

	// Emit lag warnings before deciding whether to compact.
	if c.maxLagBytes > 0 {
		for id, off := range offsets {
			if lag := fileSize - off; lag > c.maxLagBytes {
				c.log.Warn("subscriber is lagging behind channel file",
					"subscriber", id, "lag_bytes", lag, "max_lag_bytes", c.maxLagBytes)
			}
		}
	}

	if minOffset <= threshold {
		return nil // not enough consumed data to warrant compaction
	}

	// Pause the writer, copy minOffset..EOF to a temp file, rename it over
	// the channel file, then update all subscriber offsets. All of this
	// happens inside the pause window so no writes occur during the swap.
	err = pw.PauseAndSwap(func() error {
		src, err := os.Open(channelPath)
		if err != nil {
			return fmt.Errorf("open channel file for compaction: %w", err)
		}
		defer src.Close()

		if _, err := src.Seek(minOffset, io.SeekStart); err != nil {
			return fmt.Errorf("seek to min offset during compaction: %w", err)
		}

		tmp, err := os.CreateTemp(filepath.Dir(channelPath), ".compact-*")
		if err != nil {
			return fmt.Errorf("create temp file for compaction: %w", err)
		}
		tmpName := tmp.Name()

		if _, err := io.Copy(tmp, src); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return fmt.Errorf("copy channel data during compaction: %w", err)
		}
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			os.Remove(tmpName)
			return fmt.Errorf("fsync temp file during compaction: %w", err)
		}
		tmp.Close()

		if err := os.Rename(tmpName, channelPath); err != nil {
			os.Remove(tmpName)
			return fmt.Errorf("rename compacted file: %w", err)
		}

		// Adjust all subscriber offsets by the amount removed.
		for id, off := range offsets {
			newOff := off - minOffset
			if err := WriteOffset(filepath.Join(c.offsetDir, id+".offset"), newOff); err != nil {
				// Log but continue — worst case is duplicate delivery on restart,
				// which is acceptable under at-least-once semantics.
				c.log.Error("adjust subscriber offset after compaction",
					"subscriber", id, "err", err)
			}
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	c.log.Warn("channel file compacted",
		"path", channelPath, "bytes_removed", minOffset, "bytes_remaining", fileSize-minOffset)
	return nil
}
