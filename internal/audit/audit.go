// Package audit provides a structured audit writer for cross-hub forwarding events.
package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Event name constants for all audited federation events.
const (
	EventForward            = "forward"
	EventPolicyViolation    = "policy_violation"
	EventReplayGap          = "replay_gap"
	EventPeerConnected      = "peer_connected"
	EventPeerDisconnected   = "peer_disconnected"
	EventClientConnected    = "client_connected"
	EventClientRejected     = "client_rejected"
	EventClientDrain        = "client_drain"
	EventPolicyReloaded     = "policy_reloaded"
	EventPolicyReloadFailed = "policy_reload_failed"
)

// Event is a structured audit record. Only Event and Ts are required.
type Event struct {
	Ts        time.Time `json:"ts"`
	Event     string    `json:"event"`
	MessageID string    `json:"message_id,omitempty"`
	Channel   string    `json:"channel,omitempty"`
	Direction string    `json:"direction,omitempty"`
	Peer      string    `json:"peer,omitempty"`
	PeerAddr  string    `json:"peer_addr,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

// Logger is the structured logging interface used internally.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// AuditLogger is the interface satisfied by AuditWriter.
//
//nolint:revive // exported type name in exported package is intentional
type AuditLogger interface {
	Log(event Event) error
	Close() error
}

const auditFileName = "audit.jsonl"
const eventChannelCap = 1000
const dropWarnInterval = 5 * time.Second

// AuditWriter writes Event records to a rotating audit.jsonl file.
//
//nolint:revive // exported type name in exported package is intentional
type AuditWriter struct {
	dir       string
	maxSizeB  int64
	maxFiles  int
	logger    Logger
	ch        chan Event
	wg        sync.WaitGroup
	dropCount atomic.Int64 // events dropped due to full channel
}

// NewAuditWriter creates and starts an AuditWriter. dir must exist.
// maxSizeMB is the rotation threshold per file. maxFiles is the maximum
// number of rotated files retained (oldest deleted when exceeded).
func NewAuditWriter(dir string, maxSizeMB, maxFiles int, logger Logger) (*AuditWriter, error) {
	//nolint:gosec // G301: 0o755 is appropriate for shared data directories
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("audit: mkdir %s: %w", dir, err)
	}

	aw := &AuditWriter{
		dir:      dir,
		maxSizeB: int64(maxSizeMB) * 1024 * 1024,
		maxFiles: maxFiles,
		logger:   logger,
		ch:       make(chan Event, eventChannelCap),
	}

	aw.wg.Add(1)
	go aw.run()
	return aw, nil
}

// Log enqueues an event for writing. If the channel is full the event is
// dropped — audit must not block callers. Drops are counted and reported
// periodically via the structured logger and immediately to stderr.
func (aw *AuditWriter) Log(event Event) error {
	if event.Ts.IsZero() {
		event.Ts = time.Now().UTC()
	}
	select {
	case aw.ch <- event:
	default:
		aw.dropCount.Add(1)
		_, _ = fmt.Fprintf(os.Stderr, "audit: channel full, dropping event %q\n", event.Event)
	}
	return nil
}

// warnDrops emits a structured warning if any events were dropped since the
// last call. The counter is swapped atomically so no drops are double-counted.
func (aw *AuditWriter) warnDrops() {
	if n := aw.dropCount.Swap(0); n > 0 {
		aw.logger.Warn("audit: events dropped due to full channel", "count", n)
	}
}

// Close drains the event channel and closes the underlying file cleanly.
func (aw *AuditWriter) Close() error {
	close(aw.ch)
	aw.wg.Wait()
	return nil
}

// run is the internal goroutine that serialises all writes.
func (aw *AuditWriter) run() {
	defer aw.wg.Done()

	path := filepath.Join(aw.dir, auditFileName)
	//nolint:gosec // G302: audit logs are not sensitive, 0o644 is intentional
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		aw.logger.Error("audit: failed to open file", "err", err)
		// Drain channel to unblock senders before returning
		//nolint:revive // empty-block is intentional for channel draining
		for range aw.ch {
		}
		return
	}

	// Track current file size from its existing content.
	var currentSize int64
	if fi, err := f.Stat(); err == nil {
		currentSize = fi.Size()
	}

	ticker := time.NewTicker(dropWarnInterval)
	defer ticker.Stop()

	for {
		select {
		case event, ok := <-aw.ch:
			if !ok {
				// Channel closed: emit any final drop warning and shut down.
				aw.warnDrops()
				if err := f.Close(); err != nil {
					aw.logger.Error("audit: close on shutdown", "err", err)
				}
				return
			}

			data, err := json.Marshal(event)
			if err != nil {
				aw.logger.Error("audit: marshal error", "err", err)
				continue
			}
			if _, err := f.Write(data); err != nil {
				aw.logger.Error("audit: write error", "err", err)
				continue
			}
			if _, err := f.Write([]byte{'\n'}); err != nil {
				aw.logger.Error("audit: newline error", "err", err)
				continue
			}
			currentSize += int64(len(data)) + 1 // +1 for newline

			if aw.maxSizeB > 0 && currentSize >= aw.maxSizeB {
				if err := f.Close(); err != nil {
					aw.logger.Error("audit: close before rotation", "err", err)
				}
				if err := aw.rotate(); err != nil {
					aw.logger.Error("audit: rotation failed", "err", err)
				}
				//nolint:gosec // G302/G304: audit logs are not sensitive, 0o644 intentional
				f, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec
				if err != nil {
					aw.logger.Error("audit: reopen after rotation", "err", err)
					// Drain channel to unblock senders before returning
					//nolint:revive // empty-block is intentional for channel draining
					for range aw.ch {
					}
					return
				}
				currentSize = 0
			}

		case <-ticker.C:
			aw.warnDrops()
		}
	}
}

// rotate renames audit.jsonl → audit.jsonl.1, shifts .1 → .2, etc.,
// then deletes any file beyond maxFiles.
func (aw *AuditWriter) rotate() error {
	base := filepath.Join(aw.dir, auditFileName)

	// Shift existing rotated files: .N → .(N+1), working backwards.
	for i := aw.maxFiles - 1; i >= 1; i-- {
		src := fmt.Sprintf("%s.%d", base, i)
		dst := fmt.Sprintf("%s.%d", base, i+1)
		if _, err := os.Stat(src); os.IsNotExist(err) {
			continue
		}
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("audit: rotate %s → %s: %w", src, dst, err)
		}
	}

	// Rename the live file to .1.
	if _, err := os.Stat(base); err == nil {
		if err := os.Rename(base, base+".1"); err != nil {
			return fmt.Errorf("audit: rotate live file: %w", err)
		}
	}

	// Delete any file beyond maxFiles.
	if aw.maxFiles > 0 {
		excess := fmt.Sprintf("%s.%d", base, aw.maxFiles+1)
		if err := os.Remove(excess); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("audit: remove excess file %s: %w", excess, err)
		}
	}

	return nil
}
