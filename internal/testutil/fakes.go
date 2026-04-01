// Package testutil provides shared test helpers used across internal packages.
// It must not be imported by production code.
package testutil

import (
	"fmt"
	"strings"
	"sync"
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
