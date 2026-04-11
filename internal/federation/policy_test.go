//nolint:gosec // test file: G306
package federation_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wu/keyop-messenger/internal/federation"
)

// ---- AtomicPolicy -----------------------------------------------------------

func TestAllowForwardExactMatch(t *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Forward: []string{"orders", "billing"},
	})
	assert.True(t, ap.AllowForward("orders"))
	assert.True(t, ap.AllowForward("billing"))
	assert.False(t, ap.AllowForward("shipping"))
}

func TestAllowReceiveExactMatch(t *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Receive: []string{"events"},
	})
	assert.True(t, ap.AllowReceive("events"))
	assert.False(t, ap.AllowReceive("orders"))
}

func TestAllowForwardEmptyList(t *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{})
	// Forward: empty list means "don't forward to anyone".
	assert.False(t, ap.AllowForward("anything"))
	// Receive: empty list means "accept from anyone" (no allowlist = unrestricted).
	assert.True(t, ap.AllowReceive("anything"))
}

func TestAtomicPolicySwapRace(_ *testing.T) {
	ap := federation.NewAtomicPolicy(federation.ForwardPolicy{
		Forward: []string{"ch1"},
	})

	const readers = 50
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(readers + 1)

	// Writer: swap policy repeatedly.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			ap.Store(federation.ForwardPolicy{Forward: []string{"ch1", "ch2"}})
			ap.Store(federation.ForwardPolicy{Forward: []string{"ch1"}})
		}
	}()

	// Readers: call AllowForward concurrently — must not race.
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = ap.AllowForward("ch1")
				_ = ap.AllowForward("ch2")
			}
		}()
	}

	wg.Wait()
}

// ---- HubConfig / IsPeerAllowed ------------------------------------------

func TestIsPeerAllowed(t *testing.T) {
	cfg := federation.HubConfig{
		AllowedPeers: []federation.AllowedPeer{
			{Name: "billing-host"},
			{Name: "orders-host"},
		},
	}
	assert.True(t, cfg.IsPeerAllowed("billing-host"))
	assert.True(t, cfg.IsPeerAllowed("orders-host"))
	assert.False(t, cfg.IsPeerAllowed("unknown-host"))
	assert.False(t, cfg.IsPeerAllowed(""))
}

func TestIsPeerAllowedEmpty(t *testing.T) {
	cfg := federation.HubConfig{}
	assert.False(t, cfg.IsPeerAllowed("anyone"))
}
