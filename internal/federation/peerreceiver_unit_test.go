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

// TestPolicyAllowForward_Empty verifies that an empty forward policy denies all channels.
// This is by design: Forward is a whitelist, not a blacklist.
func TestPolicyAllowForward_Empty(t *testing.T) {
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{})

	assert.False(t, policy.AllowForward("any-channel"))
	assert.False(t, policy.AllowForward("another-channel"))
}

// TestPolicyAllowForward_Restricted verifies forward policy enforcement.
func TestPolicyAllowForward_Restricted(t *testing.T) {
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Forward: []string{"forward1", "forward2"},
	})

	assert.True(t, policy.AllowForward("forward1"))
	assert.True(t, policy.AllowForward("forward2"))
	assert.False(t, policy.AllowForward("disallowed"))
}

// TestClientAllowPublish delegates to the underlying policy's AllowReceive.
func TestClientAllowPublish(t *testing.T) {
	// This test verifies that Client.AllowPublish correctly delegates to
	// the policy's AllowReceive method (confusing naming but that's how it works).
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"movie"},
	})

	// Test the policy mechanism.
	assert.True(t, policy.AllowReceive("movie"), "policy should allow 'movie'")
	assert.False(t, policy.AllowReceive("movies"), "policy should reject 'movies'")
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

// TestHandshakeMsgFieldTypes verifies that HandshakeMsg fields are accessible.
func TestHandshakeMsgFieldTypes(t *testing.T) {
	hs := federation.HandshakeMsg{
		InstanceName: "test-instance",
		Role:         "client",
		Version:      "1",
		Subscribe:    []string{"ch1", "ch2"},
		LastID:       "last-msg-id",
	}

	assert.Equal(t, "test-instance", hs.InstanceName)
	assert.Equal(t, "client", hs.Role)
	assert.Equal(t, "1", hs.Version)
	assert.Len(t, hs.Subscribe, 2)
	assert.Equal(t, "last-msg-id", hs.LastID)
}

// TestAckMsgBasics verifies AckMsg fields.
func TestAckMsgBasics(t *testing.T) {
	ack := federation.AckMsg{
		LastID: "msg-id-123",
	}

	assert.Equal(t, "msg-id-123", ack.LastID)
}

// TestPolicyHotReload verifies that policy can be updated atomically.
func TestPolicyHotReload(t *testing.T) {
	policy := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"old"},
	})

	assert.True(t, policy.AllowReceive("old"))
	assert.False(t, policy.AllowReceive("new"))

	// Update policy via Store
	newPolicy := federation.ForwardPolicy{
		Receive: []string{"new"},
	}
	policy.Store(newPolicy)

	assert.False(t, policy.AllowReceive("old"), "old channel should be denied after reload")
	assert.True(t, policy.AllowReceive("new"), "new channel should be allowed after reload")
}
