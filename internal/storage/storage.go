// Package storage implements the per-channel file writer and related storage
// primitives. All file I/O is serialized through a single goroutine per channel
// so the OS O_APPEND guarantee covers records under PIPE_BUF; larger records
// are additionally protected by an advisory flock. This package targets POSIX
// systems (Linux, macOS) only.
package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// logger is the minimum logging interface required by this package.
// Structurally compatible with the root messenger.Logger and testutil.FakeLogger.
type logger interface {
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// payloadDecoder decodes raw JSON payloads into registered Go types.
// Structurally compatible with registry.PayloadRegistry.
type payloadDecoder interface {
	Decode(typeStr string, raw json.RawMessage) (any, error)
}

// segmentName formats the filename for a segment starting at the given
// global byte offset. Zero-padded to 20 digits so lexicographic == numeric order.
func segmentName(startOffset int64) string {
	return fmt.Sprintf("%020d.jsonl", startOffset)
}

// parseSegmentOffset extracts the global start offset encoded in a segment
// filename produced by segmentName.
func parseSegmentOffset(name string) (int64, error) {
	stem := strings.TrimSuffix(name, ".jsonl")
	if stem == name {
		return 0, fmt.Errorf("not a segment filename: %q", name)
	}
	return strconv.ParseInt(stem, 10, 64)
}

// segmentInfo describes one segment file in a channel directory.
type segmentInfo struct {
	path        string
	startOffset int64
	size        int64
	// modTime is the file's last-modification time, used as a proxy for the
	// timestamp of the newest record in the segment (age-based retention).
	modTime time.Time
}

// ChannelStreamEnd returns the byte position at the end of the channel's write
// stream: the start offset of the last segment plus its current byte size.
// This value is monotonically increasing and does not shrink when compaction
// removes consumed segments. Returns 0 if no segments exist yet.
func ChannelStreamEnd(channelDir string) (int64, error) {
	segs, err := listSegments(channelDir)
	if err != nil {
		return 0, err
	}
	if len(segs) == 0 {
		return 0, nil
	}
	last := segs[len(segs)-1]
	return last.startOffset + last.size, nil
}

// listSegments returns all segment files in channelDir sorted by start offset.
// os.ReadDir returns entries in name order, and since names are zero-padded
// decimal offsets lexicographic == numeric order.
// Returns nil (not an error) if the directory does not exist yet.
func listSegments(channelDir string) ([]segmentInfo, error) {
	entries, err := os.ReadDir(channelDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read channel directory %q: %w", channelDir, err)
	}
	var segs []segmentInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		start, err := parseSegmentOffset(e.Name())
		if err != nil {
			continue // ignore non-segment files
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		segs = append(segs, segmentInfo{
			path:        filepath.Join(channelDir, e.Name()),
			startOffset: start,
			size:        info.Size(),
			modTime:     info.ModTime(),
		})
	}
	return segs, nil
}
