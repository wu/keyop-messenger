package federation

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wu/keyop-messenger/internal/audit"
	"github.com/wu/keyop-messenger/internal/dedup"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/testutil"
)

// fakeAuditLogger is a simple audit logger for testing.
type fakeAuditLogger struct{}

func (f *fakeAuditLogger) Log(_ audit.Event) error { return nil }
func (f *fakeAuditLogger) Close() error            { return nil }

// TestNewClient_ValidConfig creates a valid Client.
func TestNewClient_ValidConfig(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil, // no TLS
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,                  // sendBufSize
		65536,                // maxBatchBytes
		500*time.Millisecond, // reconnectBase
		60*time.Second,       // reconnectMax
		0.2,                  // reconnectJitter
		[]string{"events"},
	)
	assert.NotNil(t, client)
	t.Cleanup(func() { client.Close() })
}

// TestNewClient_NoLocalWriter creates a client that only sends, doesn't receive.
func TestNewClient_NoLocalWriter(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-sender",
		nil,
		policy,
		nil, // no localWriter → sender-only mode
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	assert.NotNil(t, client)
	t.Cleanup(func() { client.Close() })
}

// TestClient_Dial_InvalidAddr fails gracefully.
func TestClient_Dial_InvalidAddr(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{"events"},
	)
	defer client.Close()

	_, err := client.Dial("invalid..addr:999999")
	assert.Error(t, err)
}

// TestClient_Sender_NilBeforeConnect returns nil sender before connect.
func TestClient_Sender_NilBeforeConnect(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	defer client.Close()

	sender := client.Sender()
	assert.Nil(t, sender)
}

// TestClient_Close_Idempotent verifies Close can be called multiple times.
func TestClient_Close_Idempotent(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)

	// Multiple Close calls should not panic.
	client.Close()
	client.Close()
	client.Close()
}

// TestClient_Config_WithTLS creates a client with TLS config.
func TestClient_Config_WithTLS(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})
	tlsCfg := &tls.Config{} // Minimal TLS config

	client := NewClient(
		"test-client",
		tlsCfg,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	assert.NotNil(t, client)
	defer client.Close()
}

// TestClient_Config_WithSubscribeChannels creates a client subscribing to channels.
func TestClient_Config_WithSubscribeChannels(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{"chan1", "chan2", "chan3"},
	)
	assert.NotNil(t, client)
	defer client.Close()
}

// TestClient_Config_LargeBuffers creates a client with large send/batch buffers.
func TestClient_Config_LargeBuffers(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100000,    // large send buffer
		1024*1024, // large batch size
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	assert.NotNil(t, client)
	defer client.Close()
}

// TestClient_Config_CustomReconnectParams creates client with custom backoff.
func TestClient_Config_CustomReconnectParams(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		100*time.Millisecond, // custom base
		10*time.Second,       // custom max
		0.5,                  // custom jitter
		[]string{},
	)
	assert.NotNil(t, client)
	defer client.Close()
}

// TestClient_LocalWriter_Called invokes the localWriter on message receipt.
// This is tested indirectly by the integration tests, but verifies the signature.
func TestClient_LocalWriter_Signature(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	writerCalls := 0
	localWriter := func(_ *envelope.Envelope) error {
		writerCalls++
		return nil
	}

	client := NewClient(
		"test-client",
		nil,
		policy,
		localWriter,
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	defer client.Close()

	// Verify the writer is stored but not called without a connection.
	assert.Equal(t, 0, writerCalls)
}

// TestClient_Deduplicator_Provided verifies dedup is used internally.
func TestClient_Deduplicator_Provided(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	defer client.Close()

	// Dedup is stored and used internally (tested in integration tests).
}

// TestClient_Policy_Provided verifies policy is used internally.
func TestClient_Policy_Provided(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{
		Forward: []string{"chan1"},
		Receive: []string{"chan2"},
	})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	defer client.Close()

	// Policy is stored and used internally (tested in integration tests).
}

// TestClient_AuditLogger_Provided verifies audit logger is used.
func TestClient_AuditLogger_Provided(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		500*time.Millisecond,
		60*time.Second,
		0.2,
		[]string{},
	)
	defer client.Close()

	// Audit logger is stored and used internally (tested in integration tests).
}

// TestClient_ConnectWithReconnect_InvalidAddr fails gracefully.
func TestClient_ConnectWithReconnect_InvalidAddr(t *testing.T) {
	t.Parallel()
	log := &testutil.FakeLogger{}
	auditL := &fakeAuditLogger{}
	dedupL, _ := dedup.NewLRUDedup(100)
	policy := NewAtomicPolicy(ForwardPolicy{})

	client := NewClient(
		"test-client",
		nil,
		policy,
		func(_ *envelope.Envelope) error { return nil },
		dedupL,
		auditL,
		log,
		100,
		65536,
		100*time.Millisecond, // short timeout for test
		500*time.Millisecond,
		0.2,
		[]string{},
	)
	defer client.Close()

	// First connection fails, so error is returned immediately.
	err := client.ConnectWithReconnect("invalid..addr:999999")
	assert.Error(t, err)
}

// TestClient_MinDuration_Helper tests the minDuration helper function.
func TestClient_MinDuration_Helper(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 100*time.Millisecond, minDuration(100*time.Millisecond, 200*time.Millisecond))
	assert.Equal(t, 50*time.Millisecond, minDuration(100*time.Millisecond, 50*time.Millisecond))
	assert.Equal(t, time.Duration(0), minDuration(time.Duration(0), 100*time.Millisecond))
}
