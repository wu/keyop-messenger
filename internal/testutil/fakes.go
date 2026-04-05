// Package testutil provides shared test helpers used across internal packages.
// It must not be imported by production code.
package testutil

import (
	"fmt"
	"strings"
	"sync"

	"github.com/wu/keyop-messenger/internal/envelope"
)

// FakeLogger records log calls for assertion in tests.
// Its method set is a superset of every local logger interface defined in
// internal packages, so it satisfies all of them structurally.
type FakeLogger struct {
	mu   sync.Mutex
	logs []string
}

func (f *FakeLogger) Debug(msg string, args ...any) { f.record("DEBUG", msg, args) }
func (f *FakeLogger) Info(msg string, args ...any)  { f.record("INFO", msg, args) }
func (f *FakeLogger) Warn(msg string, args ...any)  { f.record("WARN", msg, args) }
func (f *FakeLogger) Error(msg string, args ...any) { f.record("ERROR", msg, args) }

func (f *FakeLogger) record(level, msg string, args []any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry := level + ": " + msg
	if len(args) > 0 {
		entry += " " + fmt.Sprint(args...)
	}
	f.logs = append(f.logs, entry)
}

// HasWarn reports whether any WARN-level log entry contains substr.
func (f *FakeLogger) HasWarn(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.logs {
		if strings.HasPrefix(e, "WARN") && strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

// HasError reports whether any ERROR-level log entry contains substr.
func (f *FakeLogger) HasError(substr string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.logs {
		if strings.HasPrefix(e, "ERROR") && strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

// Entries returns a snapshot of all recorded log entries.
func (f *FakeLogger) Entries() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.logs))
	copy(out, f.logs)
	return out
}

// Reset clears all recorded entries.
func (f *FakeLogger) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs = f.logs[:0]
}

// ---- FakeChannelWriter -------------------------------------------------------

// FakeChannelWriter records Write calls for assertion in tests. It implements
// the same interface as storage.ChannelWriter via structural compatibility.
type FakeChannelWriter struct {
	mu       sync.Mutex
	written  []*envelope.Envelope
	writeErr error // if non-nil, Write returns this error
	closed   bool
}

// Write appends a copy of env to the recorded list, or returns writeErr if set.
func (f *FakeChannelWriter) Write(env *envelope.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writeErr != nil {
		return f.writeErr
	}
	cp := *env
	f.written = append(f.written, &cp)
	return nil
}

// Close marks the writer as closed.
func (f *FakeChannelWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Written returns a snapshot of all envelopes written so far.
func (f *FakeChannelWriter) Written() []*envelope.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*envelope.Envelope, len(f.written))
	copy(out, f.written)
	return out
}

// SetError configures Write to return err on every subsequent call.
func (f *FakeChannelWriter) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeErr = err
}

// IsClosed reports whether Close has been called.
func (f *FakeChannelWriter) IsClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// ---- FakeChannelWatcher -----------------------------------------------------

// FakeChannelWatcher implements ChannelWatcher for tests. Call Notify(path) to
// manually push a notification to all subscribers watching that path.
type FakeChannelWatcher struct {
	mu   sync.Mutex
	subs map[string][]chan struct{}
}

// Watch registers path and returns a notification channel.
func (f *FakeChannelWatcher) Watch(path string) (<-chan struct{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subs == nil {
		f.subs = make(map[string][]chan struct{})
	}
	ch := make(chan struct{}, 1)
	f.subs[path] = append(f.subs[path], ch)
	return ch, nil
}

// Close is a no-op.
func (f *FakeChannelWatcher) Close() error { return nil }

// Notify sends a non-blocking notification to all subscribers for path.
func (f *FakeChannelWatcher) Notify(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.subs[path] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
