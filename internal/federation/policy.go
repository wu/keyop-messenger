package federation

import (
	"fmt"
	"sync/atomic"
)

// ForwardPolicy holds the channel allowlists for a single peer connection.
// Reads are lock-free via AtomicPolicy.
type ForwardPolicy struct {
	Forward []string // channels this hub may send TO this peer
	Receive []string // channels this hub may accept FROM this peer
}

// AtomicPolicy wraps ForwardPolicy for lock-free concurrent reads and
// atomic swap on policy reload.
type AtomicPolicy struct {
	p atomic.Pointer[ForwardPolicy]
}

// NewAtomicPolicy stores fp as the initial policy.
func NewAtomicPolicy(fp ForwardPolicy) *AtomicPolicy {
	a := &AtomicPolicy{}
	a.p.Store(&fp)
	return a
}

// Store atomically replaces the current policy.
func (a *AtomicPolicy) Store(fp ForwardPolicy) {
	a.p.Store(&fp)
}

// AllowForward reports whether channel is in the Forward allowlist.
// Returns false for an empty or nil list.
func (a *AtomicPolicy) AllowForward(channel string) bool {
	fp := a.p.Load()
	if fp == nil {
		return false
	}
	for _, ch := range fp.Forward {
		if ch == channel {
			return true
		}
	}
	return false
}

// AllowReceive reports whether channel is in the Receive allowlist.
// Returns false for an empty or nil list.
func (a *AtomicPolicy) AllowReceive(channel string) bool {
	fp := a.p.Load()
	if fp == nil {
		return false
	}
	for _, ch := range fp.Receive {
		if ch == channel {
			return true
		}
	}
	return false
}

// ---- Hub configuration types -------------------------------------------------

// PeerHubConfig describes a single peer hub connection and its channel policy.
type PeerHubConfig struct {
	Addr    string   `yaml:"addr"`
	Forward []string `yaml:"forward"`
	Receive []string `yaml:"receive"`
}

// AllowedClient describes a client instance permitted to connect to this hub.
type AllowedClient struct {
	Name string `yaml:"name"`
}

// HubConfig is the runtime policy configuration for a hub instance. It is
// loaded from a YAML file and can be hot-reloaded without restarting.
type HubConfig struct {
	AllowedClients []AllowedClient `yaml:"allowed_clients"`
	PeerHubs       []PeerHubConfig `yaml:"peer_hubs"`
}

// IsClientAllowed reports whether name appears in the AllowedClients list.
func (c HubConfig) IsClientAllowed(name string) bool {
	for _, ac := range c.AllowedClients {
		if ac.Name == name {
			return true
		}
	}
	return false
}

// validate checks that the HubConfig is internally consistent:
// every peer hub must have a non-empty Addr, and peer names (Addr) must be
// unique.
func (c HubConfig) validate() error {
	seen := make(map[string]bool, len(c.PeerHubs))
	for i, ph := range c.PeerHubs {
		if ph.Addr == "" {
			return fmt.Errorf("federation: peer_hubs[%d]: addr must not be empty", i)
		}
		if seen[ph.Addr] {
			return fmt.Errorf("federation: peer_hubs: duplicate addr %q", ph.Addr)
		}
		seen[ph.Addr] = true
	}
	return nil
}
