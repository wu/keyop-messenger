package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Offset-file prefixes. A plain subscriber offset has no prefix; federation
// offsets are prefixed so the compactor can include every kind in its minimum
// and the hub's TTL sweep can match only its own.
const (
	// OffsetPrefixFedIn marks a hub-side offset for an inbound peer subscription.
	OffsetPrefixFedIn = "fed-"

	// OffsetPrefixFedOut marks a client-side offset for its own outbound publish
	// queue to a hub.
	OffsetPrefixFedOut = "fedout-"

	// offsetSuffix is the extension of a committed offset file. Files ending in
	// ".offset.tmp" are in-flight writes and are never listed.
	offsetSuffix = ".offset"
)

// Layout owns the on-disk arrangement of a data directory: which directory holds
// a channel's segments, which holds its subscriber offsets, and how an offset
// file is named. It is the only place those decisions are made, so no other
// package needs to join paths into the data directory — or can drift from the
// convention if it changes.
//
// The zero Layout is unusable; construct with NewLayout. It is immutable and
// safe for concurrent use.
type Layout struct {
	dataDir string
}

// NewLayout returns the Layout for a data directory.
func NewLayout(dataDir string) Layout { return Layout{dataDir: dataDir} }

// DataDir returns the root directory this layout describes.
func (l Layout) DataDir() string { return l.dataDir }

// ChannelDir returns the directory holding a channel's segment files.
func (l Layout) ChannelDir(channel string) string {
	return filepath.Join(l.dataDir, "channels", channel)
}

// OffsetDir returns the directory holding a channel's subscriber offset files.
func (l Layout) OffsetDir(channel string) string {
	return filepath.Join(l.dataDir, "subscribers", channel)
}

// OffsetPath returns the offset file for one reader of a channel. id is the
// reader's identity — a subscriber ID, or a prefixed peer name such as
// OffsetPrefixFedIn+peerCN — and is sanitized, since it can originate in a
// peer's certificate or a configured hub address and is otherwise joined
// straight into a path.
func (l Layout) OffsetPath(channel, id string) string {
	return filepath.Join(l.OffsetDir(channel), SanitizeForFilename(id)+offsetSuffix)
}

// OffsetFile describes one committed offset file found on disk.
type OffsetFile struct {
	// ID is the file's basename with its ".offset" suffix removed, including any
	// federation prefix — the same string OffsetPath accepts.
	ID string

	// Path is the full path to the file.
	Path string

	// ModTime is the file's last-modification time, used by age-based sweeps. It
	// is the zero time when the file could not be stat'ed; an age-based caller
	// must treat that as "unknown age" and leave the file alone.
	ModTime time.Time
}

// HasPrefix reports whether the offset belongs to the given kind, e.g.
// OffsetPrefixFedIn. The empty prefix matches every file.
func (o OffsetFile) HasPrefix(prefix string) bool {
	return strings.HasPrefix(o.ID, prefix)
}

// TrimPrefix returns the ID with a federation prefix removed — the peer name or
// hub address the offset belongs to.
func (o OffsetFile) TrimPrefix(prefix string) string {
	return strings.TrimPrefix(o.ID, prefix)
}

// Channels returns the names of channels that have a subscriber directory.
// Returns nil without error when the data directory has none yet.
func (l Layout) Channels() ([]string, error) {
	subsDir := filepath.Join(l.dataDir, "subscribers")
	entries, err := os.ReadDir(subsDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read subscribers dir %q: %w", subsDir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// OffsetFiles returns every committed offset file for a channel, of every kind.
// In-flight ".offset.tmp" writes and subdirectories are skipped. Returns nil
// without error when the channel has no offset directory yet.
func (l Layout) OffsetFiles(channel string) ([]OffsetFile, error) {
	dir := l.OffsetDir(channel)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read offset dir %q: %w", dir, err)
	}
	var files []OffsetFile
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), offsetSuffix) {
			continue
		}
		// A stat failure must not drop the file from the listing: the compactor
		// derives its minimum offset from this list, and a silently missing reader
		// is a reader whose unconsumed segments become eligible for deletion. Only
		// ModTime is lost, and callers that need it treat the zero value as
		// "age unknown" rather than "infinitely old".
		var modTime time.Time
		if info, err := e.Info(); err == nil {
			modTime = info.ModTime()
		}
		files = append(files, OffsetFile{
			ID:      strings.TrimSuffix(e.Name(), offsetSuffix),
			Path:    filepath.Join(dir, e.Name()),
			ModTime: modTime,
		})
	}
	return files, nil
}

// RemoveOffsetFile deletes an offset file that came from OffsetFiles. Use it
// rather than RemoveOffset for anything obtained by listing: it deletes exactly
// the file that was listed, whereas RemoveOffset re-derives the path from the ID
// and sanitizes it. A listing can contain names written before offset filenames
// were sanitized, and for those the two resolve to *different* files — deleting
// the live offset of a healthy reader instead of the stale one that was found.
// A file that is already absent is not an error.
func (l Layout) RemoveOffsetFile(f OffsetFile) error {
	if err := os.Remove(f.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove offset file %q: %w", f.Path, err)
	}
	return nil
}

// RemoveOffset deletes one reader's offset file, named from its ID the same way
// OffsetPath does. Correct only for an ID the caller holds independently, such
// as a registered subscriber's; for one that came from a listing use
// RemoveOffsetFile. A file that is already absent is not an error.
func (l Layout) RemoveOffset(channel, id string) error {
	if err := os.Remove(l.OffsetPath(channel, id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove offset for %q/%q: %w", channel, id, err)
	}
	return nil
}
