package federation_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// TestPeerReceiverErr_NilAfterCleanClose verifies that Err() returns nil when the
// receiver exits because Close() was called.
func TestPeerReceiverErr_NilAfterCleanClose(t *testing.T) {
	stream := newMockSubClientStream()

	dd, err := dedup.NewLRUDedup(100)
	require.NoError(t, err)
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	// Pass stream.cancel so pr.Close() → streamCancel() unblocks stream.Recv().
	pr := federation.NewPeerReceiver(stream, stream.cancel, policy, dd,
		func(*envelope.Envelope) error { return nil }, nil, auditL, log, "test-peer", "self", 65536)

	pr.Close()

	select {
	case <-pr.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not exit after Close()")
	}

	assert.Nil(t, pr.Err(), "Err() should be nil after clean Close()")
}

// TestPeerReceiverErr_NonNilAfterStreamClosed verifies that Err() returns a
// non-nil error when the stream closes unexpectedly (without pr.Close()).
func TestPeerReceiverErr_NonNilAfterStreamClosed(t *testing.T) {
	stream := newMockSubClientStream()

	dd, err := dedup.NewLRUDedup(100)
	require.NoError(t, err)
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	pr := federation.NewPeerReceiver(stream, stream.cancel, policy, dd,
		func(*envelope.Envelope) error { return nil }, nil, auditL, log, "test-peer", "self", 65536)

	// Abruptly cancel the stream context (simulates server-side close).
	stream.cancel()

	select {
	case <-pr.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not exit after context cancelled")
	}

	assert.NotNil(t, pr.Err(), "Err() should be non-nil after unexpected context cancel")
}

// TestPeerReceiverErr_SafeBeforeDone verifies that Err() can be called before
// Done() closes without data races.
func TestPeerReceiverErr_SafeBeforeDone(t *testing.T) {
	stream := newMockSubClientStream()

	dd, err := dedup.NewLRUDedup(100)
	require.NoError(t, err)
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	pr := federation.NewPeerReceiver(stream, stream.cancel, policy, dd,
		func(*envelope.Envelope) error { return nil }, nil, auditL, log, "test-peer", "self", 65536)

	_ = pr.Err()

	pr.Close()
	<-pr.Done()
}
