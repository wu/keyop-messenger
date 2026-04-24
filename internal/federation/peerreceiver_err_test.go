package federation_test

import (
	"sync"
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
// receiver exits because Close() was called (not due to a connection error).
func TestPeerReceiverErr_NilAfterCleanClose(t *testing.T) {
	srv, cli := newWSPair(t)
	_ = srv // keep server alive until test cleanup

	dd, err := dedup.NewLRUDedup(100)
	require.NoError(t, err)
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	pr := federation.NewPeerReceiver(cli, &sync.Mutex{}, policy, dd,
		func(*envelope.Envelope) error { return nil }, auditL, log, "test-peer", 65536)

	pr.Close()

	select {
	case <-pr.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not exit after Close()")
	}

	assert.Nil(t, pr.Err(), "Err() should be nil after clean Close()")
}

// TestPeerReceiverErr_NonNilAfterUnexpectedDisconnect verifies that Err() returns a
// non-nil error when the underlying connection is closed without calling Close() first.
func TestPeerReceiverErr_NonNilAfterUnexpectedDisconnect(t *testing.T) {
	srv, cli := newWSPair(t)

	dd, err := dedup.NewLRUDedup(100)
	require.NoError(t, err)
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	pr := federation.NewPeerReceiver(cli, &sync.Mutex{}, policy, dd,
		func(*envelope.Envelope) error { return nil }, auditL, log, "test-peer", 65536)

	// Close the server side without notifying the receiver — forces an unexpected read error.
	_ = srv.Close()

	select {
	case <-pr.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not exit after remote disconnect")
	}

	assert.NotNil(t, pr.Err(), "Err() should be non-nil after unexpected server close")
}

// TestPeerReceiverErr_SafeBeforeDone verifies that Err() can be called before Done()
// closes without data races (though the value may be nil or stale).
func TestPeerReceiverErr_SafeBeforeDone(t *testing.T) {
	srv, cli := newWSPair(t)
	_ = srv

	dd, err := dedup.NewLRUDedup(100)
	require.NoError(t, err)
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLog{}

	pr := federation.NewPeerReceiver(cli, &sync.Mutex{}, policy, dd,
		func(*envelope.Envelope) error { return nil }, auditL, log, "test-peer", 65536)

	// Calling Err() before Done() should not race; value is nil while running.
	_ = pr.Err()

	pr.Close()
	<-pr.Done()
}
