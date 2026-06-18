package federation

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/storage"
)

// sendReq carries one batch of envelopes (all from the same channel) from a
// channelReader to the clientCoordinator. The coordinator closes doneCh after
// the remote peer acknowledges the batch.
type sendReq struct {
	channel   string
	rawLines  [][]byte      // original JSONL bytes; sent directly over the wire
	newOffset int64         // global byte offset just past the last line in this batch
	doneCh    chan struct{} // closed by the coordinator once the peer ack arrives
}

// chanSegInfo describes one JSONL segment file within a channel directory.
type chanSegInfo struct {
	path        string
	startOffset int64
	size        int64
}

// listChannelSegments returns the segment files in channelDir sorted by start
// offset (ascending, guaranteed by zero-padded filenames). Returns nil without
// error when the directory does not exist yet.
func listChannelSegments(channelDir string) ([]chanSegInfo, error) {
	entries, err := os.ReadDir(channelDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("channelReader: read dir %q: %w", channelDir, err)
	}
	var segs []chanSegInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		start, err := strconv.ParseInt(strings.TrimSuffix(name, ".jsonl"), 10, 64)
		if err != nil {
			continue // not a segment file
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		segs = append(segs, chanSegInfo{
			path:        filepath.Join(channelDir, name),
			startOffset: start,
			size:        info.Size(),
		})
	}
	return segs, nil
}

// channelReader delivers envelopes from one channel's segment files to a
// clientCoordinator for a single peer. One instance is created per
// (peer, subscribed-channel) pair.
//
// The reader goroutine blocks on notifyCh; the hub calls notify() after every
// write to this channel. On wake-up the reader reads a batch, submits it to the
// coordinator via requestCh, waits for doneCh to close (meaning the peer acked
// the batch), then persists the new byte offset and loops.
type channelReader struct {
	peerName      string
	channel       string
	channelDir    string
	offsetPath    string
	maxBatchBytes int
	requestCh     chan<- sendReq
	notifyCh      chan struct{}
	log           logger

	// offset is the current global byte position; only read/written from the
	// reader goroutine so no mutex is required.
	offset int64

	stop chan struct{}
	done chan struct{}
}

// newChannelReader constructs a channelReader and initialises its byte offset.
//
// offsetDir is the subscribers/{channel}/ directory; the offset file is named
// "{offsetPrefix}{peerName}.offset" within it. The hub uses prefix "fed-" for
// inbound peer subscriptions; the client uses prefix "fedout-" for its own
// outbound publish queue. Both are included in the compactor's minimum-offset
// calculation. The hub TTL sweep only matches the "fed-" prefix, so client
// outbound offsets are not affected by it on a colocated process.
//
// If the offset file already exists the reader resumes from the stored position.
// If it does not exist (first connection) the reader starts at the current end
// of the channel so the peer only receives messages published after it connects.
func newChannelReader(
	peerName, channel, channelDir, offsetDir, offsetPrefix string,
	maxBatchBytes int,
	requestCh chan<- sendReq,
	log logger,
) (*channelReader, error) {
	//nolint:gosec // G301: shared data directory; 0755 is appropriate
	if err := os.MkdirAll(offsetDir, 0o755); err != nil {
		return nil, fmt.Errorf("newChannelReader: mkdir %q: %w", offsetDir, err)
	}
	offsetPath := filepath.Join(offsetDir, offsetPrefix+peerName+".offset")

	var offset int64
	if storage.OffsetFileExists(offsetPath) {
		var err error
		offset, err = storage.ReadOffset(offsetPath)
		if err != nil {
			return nil, fmt.Errorf("newChannelReader %s/%s: read offset: %w", peerName, channel, err)
		}
	} else {
		// New subscriber: start at the end so the peer only receives new messages.
		segs, err := listChannelSegments(channelDir)
		if err != nil {
			return nil, fmt.Errorf("newChannelReader %s/%s: list segments: %w", peerName, channel, err)
		}
		if len(segs) > 0 {
			last := segs[len(segs)-1]
			offset = last.startOffset + last.size
		}
		if err := storage.WriteOffset(offsetPath, offset); err != nil {
			return nil, fmt.Errorf("newChannelReader %s/%s: write initial offset: %w", peerName, channel, err)
		}
	}

	cr := &channelReader{
		peerName:      peerName,
		channel:       channel,
		channelDir:    channelDir,
		offsetPath:    offsetPath,
		maxBatchBytes: maxBatchBytes,
		requestCh:     requestCh,
		notifyCh:      make(chan struct{}, 1),
		log:           log,
		offset:        offset,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	// Self-notify so run() drains any backlog already present in the channel
	// file on startup. New subscribers (offset == stream end) see nothing to
	// drain and immediately go back to sleep; resuming subscribers — most
	// importantly client-side outbound readers reconnecting after a hub
	// disconnect — pick up any data published while the previous coordinator
	// was disconnected.
	cr.notify()
	return cr, nil
}

// notify wakes the reader goroutine without blocking. Coalesced: if a
// notification is already pending the new one is silently dropped (the
// goroutine will drain all available data on the next wake-up anyway).
func (cr *channelReader) notify() {
	select {
	case cr.notifyCh <- struct{}{}:
	default:
	}
}

// start launches the reader goroutine. Call close() to stop it.
func (cr *channelReader) start() {
	go cr.run()
}

// close stops the reader goroutine and waits for it to exit. Safe to call once.
func (cr *channelReader) close() {
	select {
	case <-cr.stop:
	default:
		close(cr.stop)
	}
	<-cr.done
}

func (cr *channelReader) run() {
	defer close(cr.done)
	for {
		select {
		case <-cr.stop:
			return
		case <-cr.notifyCh:
		}
		cr.drainAndSend()
	}
}

// drainAndSend reads all available envelopes in batches, sending each batch to
// the coordinator and waiting for the ack before reading the next one.
func (cr *channelReader) drainAndSend() {
	for {
		rawLines, newOffset, hasMore, ok := cr.readBatch()
		if !ok {
			return // error already logged
		}
		if len(rawLines) == 0 {
			// No deliverable records. readBatch may still have advanced past
			// corrupt or oversized records it dropped; persist that progress so we
			// don't re-scan (and re-drop) them on every notification.
			if newOffset > cr.offset {
				cr.offset = newOffset
				if err := storage.WriteOffset(cr.offsetPath, newOffset); err != nil {
					cr.log.Error("channelReader: persist offset",
						"channel", cr.channel, "peer", cr.peerName, "err", err)
				}
				if hasMore {
					continue
				}
			}
			return // nothing new
		}

		doneCh := make(chan struct{})
		req := sendReq{
			channel:   cr.channel,
			rawLines:  rawLines,
			newOffset: newOffset,
			doneCh:    doneCh,
		}

		// Submit batch to coordinator; bail if we are being stopped.
		select {
		case cr.requestCh <- req:
		case <-cr.stop:
			return
		}

		// Wait for the coordinator to confirm the remote peer acked the batch.
		select {
		case <-doneCh:
		case <-cr.stop:
			return
		}

		// Persist the new offset atomically before reading the next batch.
		cr.offset = newOffset
		if err := storage.WriteOffset(cr.offsetPath, newOffset); err != nil {
			cr.log.Error("channelReader: persist offset",
				"channel", cr.channel, "peer", cr.peerName, "err", err)
			// Continue delivering; the offset will be retried on the next notification.
		}

		if !hasMore {
			return
		}
		// Batch was size-limited; more data is available — loop immediately.
	}
}

// readBatch scans segment files from the current offset, accumulating JSONL
// lines up to maxBatchBytes. Returns:
//   - rawLines: the raw bytes of each envelope line to send
//   - newOffset: the global byte position just past the last included line
//   - hasMore: true when the batch was cut short by the size limit (caller
//     should loop without waiting for the next notify)
//   - ok: false on I/O error (already logged); caller should return immediately
func (cr *channelReader) readBatch() (rawLines [][]byte, newOffset int64, hasMore bool, ok bool) {
	segs, err := listChannelSegments(cr.channelDir)
	if err != nil {
		cr.log.Error("channelReader: list segments", "channel", cr.channel, "err", err)
		return nil, cr.offset, false, false
	}

	newOffset = cr.offset
	totalBytes := 0

	for i, seg := range segs {
		segEnd := seg.startOffset + seg.size
		if segEnd <= cr.offset {
			continue // already consumed this entire segment
		}

		f, err := os.Open(seg.path)
		if err != nil {
			cr.log.Error("channelReader: open segment", "path", seg.path, "err", err)
			return nil, cr.offset, false, false
		}

		// Seek to the correct position within the segment.
		lineOffset := cr.offset
		if seg.startOffset > cr.offset {
			lineOffset = seg.startOffset
		}
		if _, err := f.Seek(lineOffset-seg.startOffset, io.SeekStart); err != nil {
			cr.log.Error("channelReader: seek", "path", seg.path, "err", err)
			_ = f.Close()
			return nil, cr.offset, false, false
		}

		// Allow reading lines up to the gRPC frame limit so a record that the
		// transport could carry is never truncated; records beyond that are
		// handled by the bufio.ErrTooLong branch below. Never allocate an initial
		// buffer larger than that ceiling.
		maxLine := grpcMessageLimit(cr.maxBatchBytes)
		initBuf := 64 * 1024
		if maxLine < initBuf {
			initBuf = maxLine
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, initBuf), maxLine)

		for scanner.Scan() {
			line := scanner.Bytes()
			next := lineOffset + int64(len(line)) + 1 // +1 for the '\n'

			// Drop records too large to ever fit in a gRPC frame. Advance past
			// them so a single oversized message cannot wedge this channel in an
			// endless disconnect/redeliver loop.
			if len(line) > maxRecordBytes {
				cr.log.Error("channelReader: dropping oversized record",
					"channel", cr.channel, "peer", cr.peerName,
					"bytes", len(line), "max", maxRecordBytes)
				lineOffset = next
				newOffset = next
				continue
			}

			// Stop before this line if it would overflow the batch (always include
			// at least one line so we make forward progress even on huge messages).
			if cr.maxBatchBytes > 0 && totalBytes+len(line) > cr.maxBatchBytes && len(rawLines) > 0 {
				_ = f.Close()
				return rawLines, newOffset, true, true
			}

			// Validate: skip corrupt records but still advance the offset.
			if _, err := envelope.Unmarshal(line); err != nil {
				cr.log.Error("channelReader: unmarshal corrupt record",
					"channel", cr.channel, "err", err)
				lineOffset = next
				newOffset = next
				continue
			}

			lineCopy := make([]byte, len(line))
			copy(lineCopy, line)
			rawLines = append(rawLines, lineCopy)
			totalBytes += len(line)
			lineOffset = next
			newOffset = next
		}
		if scanErr := scanner.Err(); scanErr != nil {
			if errors.Is(scanErr, bufio.ErrTooLong) {
				// The record is larger than the scan buffer (and thus larger than
				// any frame we could send). Locate its terminating newline and skip
				// past it so the reader makes forward progress instead of looping.
				skipTo, found, serr := offsetPastNextLine(seg.path, lineOffset-seg.startOffset, seg.startOffset)
				_ = f.Close()
				if serr != nil {
					cr.log.Error("channelReader: locate oversized record end",
						"channel", cr.channel, "err", serr)
					return rawLines, newOffset, len(rawLines) > 0, true
				}
				if !found {
					// No terminating newline yet — the record is still being
					// written. Deliver what we have and retry on the next notify.
					return rawLines, newOffset, false, true
				}
				cr.log.Error("channelReader: dropping oversized record",
					"channel", cr.channel, "peer", cr.peerName,
					"from", lineOffset, "to", skipTo)
				newOffset = skipTo
				// Loop again from the advanced offset to pick up later records.
				return rawLines, newOffset, true, true
			}
			cr.log.Error("channelReader: scan", "path", seg.path, "err", scanErr)
		}
		_ = f.Close()

		// Advance past any gap between this segment and the next.
		if i+1 < len(segs) && newOffset < segs[i+1].startOffset {
			newOffset = segs[i+1].startOffset
		}
	}

	return rawLines, newOffset, false, true
}

// offsetPastNextLine scans segPath from relStart looking for the next '\n' and
// returns the global offset (segStart + position just past the newline). found
// is false when EOF is reached with no newline — the record is still being
// written, so the caller should wait rather than skip a partial line.
func offsetPastNextLine(segPath string, relStart, segStart int64) (globalOff int64, found bool, err error) {
	f, err := os.Open(segPath) // #nosec G304 -- segPath is an internally-derived segment path
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(relStart, io.SeekStart); err != nil {
		return 0, false, err
	}

	buf := make([]byte, 256*1024)
	pos := relStart
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			if idx := bytes.IndexByte(buf[:n], '\n'); idx >= 0 {
				return segStart + pos + int64(idx) + 1, true, nil
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
