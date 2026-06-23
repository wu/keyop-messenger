// Shared test helpers used by both unit and integration tests in package
// federation_test. No build tag so unit test runs always include this file.
package federation_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/envelope"
)

// fakeAuditLog is a thread-safe audit logger used by multiple test files.
type fakeAuditLog struct {
	mu     sync.Mutex
	events []audit.Event
}

func (f *fakeAuditLog) Log(ev audit.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeAuditLog) Close() error { return nil }
func (f *fakeAuditLog) count(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, ev := range f.events {
		if ev.Event == name {
			n++
		}
	}
	return n
}
func (f *fakeAuditLog) has(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ev := range f.events {
		if ev.Event == name {
			return true
		}
	}
	return false
}

// makeTestEnv creates a simple test envelope for peerreceiver and hub tests.
func makeTestEnv(t *testing.T, channel, id string) envelope.Envelope {
	t.Helper()
	env, err := envelope.NewEnvelope(channel, "test-origin", "test.Type", map[string]any{"id": id})
	require.NoError(t, err)
	return env
}
