package storage

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
)

// SkipReason says why a record could not be delivered and was stepped over.
type SkipReason int

const (
	// SkipOversized is a complete record larger than CursorOpts.MaxRecordBytes.
	// It was read successfully; the consumer simply cannot carry it.
	SkipOversized SkipReason = iota

	// SkipUnscannable is a record larger than CursorOpts.ScanLimit, so it could
	// not be held in the scan buffer at all. Its end was located by scanning for
	// the next newline.
	SkipUnscannable
)

// SkipInfo describes one record the cursor stepped over. The cursor does not log;
// each consumer reports skips in its own terms via CursorOpts.OnSkip.
type SkipInfo struct {
	Reason     SkipReason
	Segment    string
	Offset     int64 // global offset of the skipped record
	NextOffset int64 // global offset just past it
	Len        int   // record length, or 0 when it was never held in memory
}

// CursorOpts configures a Cursor.
type CursorOpts struct {
	// MaxRecordBytes is the largest record the consumer can accept. Larger
	// records are skipped rather than returned. 0 means no limit.
	MaxRecordBytes int

	// ScanLimit is the ceiling on the scan buffer, and so on the record size the
	// cursor can hold at all. It must be at least MaxRecordBytes; a record above
	// it is skipped as SkipUnscannable. 0 selects defaultScanLimit.
	ScanLimit int

	// InitialBufSize is the scan buffer's starting size. 0 selects
	// defaultScanInitialBufSize.
	InitialBufSize int

	// CommittedEnd reports the channel's committed end, bounding every read so
	// bytes of a record still being appended are never in the buffer. nil, or a
	// zero return, means "unknown" and reads run to EOF — where the complete-line
	// rule is what keeps a partial tail from being consumed.
	CommittedEnd func() int64

	// OnSkip is called for each skipped record. Optional.
	OnSkip func(SkipInfo)
}

const (
	defaultScanLimit          = 10 * 1024 * 1024
	defaultScanInitialBufSize = 64 * 1024
	newlineSearchChunk        = 256 * 1024
)

// Record is one complete record read from a channel.
type Record struct {
	// Bytes is the record without its terminating newline. It points into the
	// cursor's scan buffer and is only valid until the next call to Next: a
	// consumer that retains it must copy it.
	Bytes []byte

	// Offset is the record's global byte offset; NextOffset is just past its
	// terminating newline — the offset a consumer persists once it has handled
	// this record.
	Offset     int64
	NextOffset int64

	// Segment is the file the record came from, for diagnostics.
	Segment string
}

// Cursor iterates the complete records of a channel from a global byte offset.
//
// It is the only reader of segment files: it owns where records begin and end,
// how far it is safe to read, and what to do about a record it cannot carry.
// Consumers supply an offset and get back framed records.
//
// A cursor owns its scan buffer, so keep one per reader and Reset it at the
// start of each pass rather than creating one per wake-up — the buffer is tens
// of kilobytes and allocating it per pass dominates a reader's allocation.
// A Cursor is not safe for concurrent use.
type Cursor struct {
	channelDir string
	opts       CursorOpts

	buf  []byte // reused scan buffer
	segs []segmentInfo

	// limitReader bounds a scan to the committed end. It is a reused field
	// rather than an io.LimitReader call per segment open, which allocates on
	// the read hot path.
	limitReader io.LimitedReader

	// Open segment, if any.
	file     *os.File
	scanner  *bufio.Scanner
	segPath  string
	segStart int64
	segEnd   int64

	offset int64
}

// NewCursor returns a cursor over channelDir. It performs no I/O; call Reset to
// position it.
func NewCursor(channelDir string, opts CursorOpts) *Cursor {
	if opts.ScanLimit <= 0 {
		opts.ScanLimit = defaultScanLimit
	}
	if opts.InitialBufSize <= 0 {
		opts.InitialBufSize = defaultScanInitialBufSize
	}
	if opts.InitialBufSize > opts.ScanLimit {
		opts.InitialBufSize = opts.ScanLimit
	}
	return &Cursor{
		channelDir: channelDir,
		opts:       opts,
		buf:        make([]byte, opts.InitialBufSize),
	}
}

// Reset positions the cursor at a global byte offset and re-reads the channel's
// segment list. Call it at the start of every pass: segments are created by the
// writer and deleted by compaction between passes.
func (c *Cursor) Reset(offset int64) error {
	c.closeSegment()
	c.offset = offset
	segs, err := listSegments(c.channelDir)
	if err != nil {
		return err
	}
	c.segs = segs
	return nil
}

// CommittedEndFunc sets the committed-end accessor after construction, for a
// consumer that learns it later than it builds its cursor. Not safe to call once
// the cursor is in use.
func (c *Cursor) CommittedEndFunc(fn func() int64) { c.opts.CommittedEnd = fn }

// OnSkipFunc sets the skip callback after construction, for a consumer whose
// reporting needs a reference to itself. Not safe to call once the cursor is in
// use.
func (c *Cursor) OnSkipFunc(fn func(SkipInfo)) { c.opts.OnSkip = fn }

// EarliestOffset returns the first offset still on disk — the start of the
// oldest surviving segment — or -1 when the channel has no segments. Valid after
// Reset. A consumer parked below it has been undercut by compaction and has lost
// those messages; the cursor itself steps forward to this offset either way.
func (c *Cursor) EarliestOffset() int64 {
	if len(c.segs) == 0 {
		return -1
	}
	return c.segs[0].startOffset
}

// Offset returns the cursor's current position: just past the last record Next
// returned, or past anything it skipped. It is not a consumer's persisted
// offset — a consumer that stops mid-pass keeps its own.
func (c *Cursor) Offset() int64 { return c.offset }

// Close releases the open segment file. The cursor may be Reset and reused after
// Close.
func (c *Cursor) Close() error {
	c.closeSegment()
	return nil
}

// Next returns the next complete record. ok is false when nothing more is
// available right now — the cursor reached the end of the data, the committed
// end, or a record whose terminating newline has not been written yet. In every
// such case the position is left where the next pass should resume.
//
// The returned Record.Bytes is only valid until the next call.
func (c *Cursor) Next() (Record, bool, error) {
	for {
		if c.scanner == nil {
			opened, err := c.openAtOffset()
			if err != nil {
				return Record{}, false, err
			}
			if !opened {
				return Record{}, false, nil
			}
		}

		if c.scanner.Scan() {
			line := c.scanner.Bytes()
			next := c.offset + int64(len(line)) + 1 // +1 for the '\n'

			// A record the consumer cannot carry is stepped over rather than
			// returned, so one poison message cannot wedge the channel.
			if c.opts.MaxRecordBytes > 0 && len(line) > c.opts.MaxRecordBytes {
				c.reportSkip(SkipInfo{
					Reason: SkipOversized, Segment: c.segPath,
					Offset: c.offset, NextOffset: next, Len: len(line),
				})
				c.offset = next
				continue
			}

			rec := Record{Bytes: line, Offset: c.offset, NextOffset: next, Segment: c.segPath}
			c.offset = next
			return rec, true, nil
		}

		scanErr := c.scanner.Err()
		switch {
		case scanErr == nil:
			segEnd := c.segEnd
			c.closeSegment()
			if c.offset < segEnd {
				// The scan ended before the segment did, so it ended on an
				// unterminated record or on the committed end. Nothing more is
				// readable now, and re-opening the same segment would spin.
				return Record{}, false, nil
			}
			continue // this segment is consumed; try the next one

		case errors.Is(scanErr, bufio.ErrTooLong):
			skipped, err := c.skipUnscannable()
			if err != nil {
				return Record{}, false, err
			}
			if !skipped {
				// No terminating newline yet: this is a record still being
				// written, not one too large to read. Hold position.
				return Record{}, false, nil
			}
			continue

		default:
			path := c.segPath
			c.closeSegment()
			return Record{}, false, fmt.Errorf("scan segment %q at %d: %w", path, c.offset, scanErr)
		}
	}
}

// openAtOffset opens the segment containing the cursor's offset, advancing over
// any gap between segments, and prepares a scanner bounded by the committed end.
// Returns false when no segment holds the offset or nothing new is committed.
func (c *Cursor) openAtOffset() (bool, error) {
	limit := c.readLimit()
	if limit == 0 {
		return false, nil // nothing committed beyond the current position
	}

	for _, seg := range c.segs {
		if seg.startOffset+seg.size <= c.offset {
			continue // entirely behind the cursor
		}
		if c.offset < seg.startOffset {
			// Gap between a sealed segment's end and the next segment's start.
			c.offset = seg.startOffset
			limit = c.readLimit()
			if limit == 0 {
				return false, nil
			}
		}

		f, err := os.Open(seg.path) // #nosec G304 -- trusted, library-constructed segment path
		if err != nil {
			return false, fmt.Errorf("open segment %q: %w", seg.path, err)
		}
		if _, err := f.Seek(c.offset-seg.startOffset, io.SeekStart); err != nil {
			_ = f.Close()
			return false, fmt.Errorf("seek segment %q to %d: %w", seg.path, c.offset, err)
		}

		var src io.Reader = f
		if limit > 0 {
			c.limitReader = io.LimitedReader{R: f, N: limit}
			src = &c.limitReader
		}
		scanner := bufio.NewScanner(src)
		scanner.Buffer(c.buf, c.opts.ScanLimit)
		scanner.Split(scanCompleteLines)

		c.file, c.scanner, c.segPath = f, scanner, seg.path
		c.segStart, c.segEnd = seg.startOffset, seg.startOffset+seg.size
		return true, nil
	}
	return false, nil
}

// readLimit returns how many bytes may be read from the current offset: the
// distance to the committed end, 0 when nothing new is committed, or -1 when the
// committed end is unknown and reads run to EOF.
func (c *Cursor) readLimit() int64 {
	if c.opts.CommittedEnd == nil {
		return -1
	}
	end := c.opts.CommittedEnd()
	if end <= 0 {
		return -1
	}
	if end <= c.offset {
		return 0
	}
	return end - c.offset
}

// skipUnscannable steps over a record too large for the scan buffer by locating
// its terminating newline. Returns false when there is no newline yet, which
// means the record is still being written rather than genuinely oversized.
func (c *Cursor) skipUnscannable() (bool, error) {
	segPath, segStart := c.segPath, c.segStart
	c.closeSegment()

	next, found, err := offsetPastNextNewline(segPath, c.offset-segStart, segStart)
	if err != nil {
		return false, fmt.Errorf("locate end of oversized record in %q at %d: %w", segPath, c.offset, err)
	}
	if !found {
		return false, nil
	}
	c.reportSkip(SkipInfo{
		Reason: SkipUnscannable, Segment: segPath,
		Offset: c.offset, NextOffset: next,
	})
	c.offset = next
	return true, nil
}

func (c *Cursor) reportSkip(info SkipInfo) {
	if c.opts.OnSkip != nil {
		c.opts.OnSkip(info)
	}
}

func (c *Cursor) closeSegment() {
	if c.file != nil {
		_ = c.file.Close()
	}
	c.file, c.scanner, c.segPath, c.segStart, c.segEnd = nil, nil, "", 0, 0
}

// offsetPastNextNewline returns the global offset just past the first '\n' at or
// after localStart within segPath. found is false when the file ends with no
// newline — the record is still being written.
func offsetPastNextNewline(segPath string, localStart, segStart int64) (int64, bool, error) {
	f, err := os.Open(segPath) // #nosec G304 -- trusted, library-constructed segment path
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(localStart, io.SeekStart); err != nil {
		return 0, false, err
	}

	buf := make([]byte, newlineSearchChunk)
	pos := localStart
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			if i := bytes.IndexByte(buf[:n], '\n'); i >= 0 {
				return segStart + pos + int64(i) + 1, true, nil
			}
			pos += int64(n)
		}
		if rerr == io.EOF {
			return 0, false, nil
		}
		if rerr != nil {
			return 0, false, rerr
		}
	}
}
