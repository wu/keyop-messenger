package federation

import (
	"fmt"
	"os"
	"sync"

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

	// destInstanceFn returns the destination peer's authenticated identity (its
	// TLS cert CN), or "" if it is not yet known. It is a function rather than a
	// fixed string because the client learns the hub's identity asynchronously
	// during the TLS handshake. When it returns a non-empty value, readBatch
	// drops any record whose path vector already contains that identity, so an
	// echo is never sent back to a peer that already holds the message. When it
	// returns "" (identity not yet known, or a non-TLS connection) filtering is
	// skipped and the receiver's own loop guard is relied on as a backstop.
	destInstanceFn func() string

	// committedEnd reports this channel's committed end — the offset just past
	// the last complete record the local writer has written. Scans are bounded by
	// it, so bytes of a record still being appended are never read. nil, or a
	// zero return, means "unknown" (no writer for this channel in this process
	// yet, which is the case after a restart until the channel next sees
	// traffic) and the scan falls back to EOF, where the complete-line split
	// function is what keeps a partial tail from being consumed.
	committedEnd func() int64

	// cursor frames records from the channel's segment files. It is owned by this
	// reader and reused across passes: it holds the scan buffer, and allocating
	// one of those per wake-up dominated this path's allocation.
	cursor *storage.Cursor

	// offset is the current global byte position; only read/written from the
	// reader goroutine so no mutex is required.
	offset int64

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
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
	layout storage.Layout,
	peerName, channel, offsetPrefix string,
	maxBatchBytes int,
	requestCh chan<- sendReq,
	destInstanceFn func() string,
	committedEndFn func() int64,
	log logger,
) (*channelReader, error) {
	// channel reaches the filesystem as a directory component. On the hub side it
	// is peer-supplied (a Subscribe request's channel list is returned verbatim
	// when the peer's allowlist is unrestricted), so it is attacker-influenced
	// data and is validated here as the last guard before MkdirAll. The callers
	// filter invalid names out earlier; this makes it impossible to bypass.
	if err := storage.ValidateChannelName(channel); err != nil {
		return nil, fmt.Errorf("newChannelReader %s: %w", peerName, err)
	}

	channelDir := layout.ChannelDir(channel)
	offsetDir := layout.OffsetDir(channel)
	// #nosec G301 -- shared data directory; 0755 is appropriate
	if err := os.MkdirAll(offsetDir, 0o755); err != nil {
		return nil, fmt.Errorf("newChannelReader: mkdir %q: %w", offsetDir, err)
	}
	// peerName is caller-controlled — on the hub side it is the peer's
	// certificate CN — so it reaches the filesystem only through OffsetPath,
	// which sanitizes it.
	offsetPath := layout.OffsetPath(channel, offsetPrefix+peerName)

	var offset int64
	if storage.OffsetFileExists(offsetPath) {
		var err error
		offset, err = storage.ReadOffset(offsetPath)
		if err != nil {
			return nil, fmt.Errorf("newChannelReader %s/%s: read offset: %w", peerName, channel, err)
		}
	} else {
		// New subscriber: start at the end so the peer only receives messages
		// published after it connects. This must be the end of the last *complete*
		// record, not the file size: the writer may be mid-append, or a crash may
		// have left a partial record that this process has not recovered yet
		// (segment recovery runs when a channel's writer is created, which is
		// lazy). Starting at a file size that includes those bytes would put the
		// reader inside a record, and every record it framed after that would be
		// garbage.
		var err error
		offset, err = storage.ChannelCommittedEnd(channelDir)
		if err != nil {
			return nil, fmt.Errorf("newChannelReader %s/%s: committed end: %w", peerName, channel, err)
		}
		if err := storage.WriteOffset(offsetPath, offset); err != nil {
			return nil, fmt.Errorf("newChannelReader %s/%s: write initial offset: %w", peerName, channel, err)
		}
	}

	// The cursor owns framing for this reader: the per-record cap is what a gRPC
	// frame can carry, and the scan ceiling sits above it so a record that is too
	// big to send can still be read far enough to be stepped over.
	cursor := storage.NewCursor(channelDir, committedEndFn, storage.CursorOpts{
		MaxRecordBytes: maxRecordBytes,
		ScanLimit:      grpcMessageLimit(maxBatchBytes),
	})

	cr := &channelReader{
		peerName:       peerName,
		channel:        channel,
		channelDir:     channelDir,
		offsetPath:     offsetPath,
		maxBatchBytes:  maxBatchBytes,
		requestCh:      requestCh,
		notifyCh:       make(chan struct{}, 1),
		log:            log,
		destInstanceFn: destInstanceFn,
		committedEnd:   committedEndFn,
		cursor:         cursor,
		offset:         offset,
		stop:           make(chan struct{}),
		done:           make(chan struct{}),
	}
	cursor.OnSkipFunc(func(info storage.SkipInfo) {
		// Advancing past a record we cannot carry is how one poison message is
		// kept from wedging the channel in an endless disconnect/redeliver loop.
		cr.log.Error("channelReader: dropping oversized record",
			"channel", cr.channel, "peer", cr.peerName, "reason", info.Reason,
			"segment", info.Segment, "from", info.Offset, "to", info.NextOffset,
			"bytes", info.Len, "max", maxRecordBytes)
	})

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

// close stops the reader goroutine and waits for it to exit. Safe to call
// multiple times and concurrently: the teardown runs exactly once and every
// caller blocks until the goroutine has exited.
func (cr *channelReader) close() {
	cr.closeOnce.Do(func() {
		close(cr.stop)
		<-cr.done
	})
}

func (cr *channelReader) run() {
	defer close(cr.done)
	// The cursor is touched only by this goroutine, so it is closed here.
	defer func() { _ = cr.cursor.Close() }()
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

// readBatch accumulates JSONL lines from the current offset up to
// maxBatchBytes. Framing — where records begin and end, how far it is safe to
// read, and what to do with a record too large to carry — belongs to the
// cursor; this loop only decides what to send. Returns:
//   - rawLines: the raw bytes of each envelope line to send
//   - newOffset: the global byte position just past the last included line
//   - hasMore: true when the batch was cut short by the size limit (caller
//     should loop without waiting for the next notify)
//   - ok: false on I/O error (already logged); caller should return immediately
func (cr *channelReader) readBatch() (rawLines [][]byte, newOffset int64, hasMore bool, ok bool) {
	if err := cr.cursor.Reset(cr.offset); err != nil {
		cr.log.Error("channelReader: list segments",
			"channel", cr.channel, "peer", cr.peerName, "dir", cr.channelDir, "err", err)
		return nil, cr.offset, false, false
	}

	newOffset = cr.offset
	totalBytes := 0

	// Resolve the destination's identity once per scan. When known, records whose
	// path vector already includes it are echoes and are dropped here so they are
	// never sent across the wire.
	var destInstance string
	if cr.destInstanceFn != nil {
		destInstance = cr.destInstanceFn()
	}

	for {
		rec, more, err := cr.cursor.Next()
		if err != nil {
			// Leave the offset unadvanced and retry on the next notify rather than
			// reading on past data we could not frame.
			cr.log.Error("channelReader: read",
				"channel", cr.channel, "peer", cr.peerName, "offset", newOffset, "err", err)
			return nil, cr.offset, false, false
		}
		if !more {
			// Trailing records the cursor stepped over are consumed; without this
			// a batch containing nothing but dropped records would report no
			// progress and rescan — and re-drop — them on every notification.
			return rawLines, cr.cursor.Offset(), false, true
		}

		// Records the cursor stepped over lie between newOffset and this record;
		// they are consumed, so count them as progress before any early return.
		if rec.Offset > newOffset {
			newOffset = rec.Offset
		}

		// Stop before this line if it would overflow the batch (always include at
		// least one line so we make forward progress even on huge messages). The
		// record is not consumed: the next pass resumes at newOffset.
		if cr.maxBatchBytes > 0 && totalBytes+len(rec.Bytes) > cr.maxBatchBytes && len(rawLines) > 0 {
			return rawLines, newOffset, true, true
		}

		// Validate: skip corrupt records but still advance the offset. Log enough
		// to locate and inspect the record afterwards — segment file, byte range,
		// length and a quoted preview of the bytes — because the json error alone
		// cannot distinguish on-disk corruption from a mis-framed offset, and the
		// record is dropped for good below.
		env, err := envelope.Unmarshal(rec.Bytes)
		if err != nil {
			cr.log.Error("channelReader: unmarshal corrupt record",
				"channel", cr.channel, "peer", cr.peerName,
				"segment", rec.Segment, "offset", rec.Offset, "next_offset", rec.NextOffset,
				"record_len", len(rec.Bytes), "record", envelope.Preview(rec.Bytes, 0),
				"err", err)
			newOffset = rec.NextOffset
			continue
		}

		// Send-side loop guard: if the destination already appears in this
		// record's path vector, forwarding it would echo the message back to a
		// peer that already holds it. Drop it before it reaches the wire and
		// advance the offset so it is not rescanned.
		if destInstance != "" && env.RouteContains(destInstance) {
			cr.log.Debug("channelReader: dropping echo (dest already in route)",
				"channel", cr.channel, "peer", cr.peerName, "dest", destInstance, "id", env.ID)
			newOffset = rec.NextOffset
			continue
		}

		// rec.Bytes is only valid until the next Next, and the batch outlives this
		// loop, so copy.
		rawLines = append(rawLines, append([]byte(nil), rec.Bytes...))
		totalBytes += len(rec.Bytes)
		newOffset = rec.NextOffset
	}
}
