package federation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wu/keyop-messenger/internal/envelope"
	"github.com/wu/keyop-messenger/internal/federation"
)

// TestPolicyAllowReceive_Empty verifies that an empty receive policy allows all channels.
func TestPolicyAllowReceive_Empty(t *testing.T) {
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})

	assert.True(t, policy.AllowReceive("any-channel"))
	assert.True(t, policy.AllowReceive("another-channel"))
	assert.True(t, policy.AllowReceive(""))
}

// TestPolicyAllowReceive_Restricted verifies that a restricted receive policy
// only allows specified channels.
func TestPolicyAllowReceive_Restricted(t *testing.T) {
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"allowed1", "allowed2"},
	})

	assert.True(t, policy.AllowReceive("allowed1"))
	assert.True(t, policy.AllowReceive("allowed2"))
	assert.False(t, policy.AllowReceive("disallowed"))
	assert.False(t, policy.AllowReceive("other"))
}

// TestPolicyHotReload verifies that policy can be updated atomically.
func TestPolicyHotReload(t *testing.T) {
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"old"},
	})

	assert.True(t, policy.AllowReceive("old"))
	assert.False(t, policy.AllowReceive("new"))

	policy.Store(federation.ForwardPolicy{Receive: []string{"new"}})

	assert.False(t, policy.AllowReceive("old"), "old channel should be denied after reload")
	assert.True(t, policy.AllowReceive("new"), "new channel should be allowed after reload")
}

// TestEnvelopeBasics verifies basic Envelope creation and field access.
func TestEnvelopeBasics(t *testing.T) {
	env, err := envelope.NewEnvelope("test-channel", "test-origin", "test.type.v1", map[string]any{"key": "value"})
	assert.NoError(t, err)
	assert.NotEmpty(t, env.ID)
	assert.Equal(t, "test-channel", env.Channel)
	assert.Equal(t, "test-origin", env.Origin)
	assert.Equal(t, "test.type.v1", env.PayloadType)
	assert.Greater(t, len(env.Payload), 0)
}
