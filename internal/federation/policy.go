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
// An empty or nil list means "accept all channels" (returns true).
func (a *AtomicPolicy) AllowReceive(channel string) bool {
	fp := a.p.Load()
	if fp == nil || len(fp.Receive) == 0 {
		return true
	}
	for _, ch := range fp.Receive {
		if ch == channel {
			return true
		}
	}
	return false
}

// ---- Federation configuration types -----------------------------------------

// AllowedPeer describes a peer instance permitted to connect to a hub.
type AllowedPeer struct {
	Name      string   `yaml:"name"`
	Subscribe []string `yaml:"subscribe"`
	Publish   []string `yaml:"publish"`
}

// HubConfig holds the federation policy for a hub instance.
type HubConfig struct {
	AllowedPeers []AllowedPeer `yaml:"allowed_peers"`
}

// IsPeerAllowed reports whether name appears in the AllowedPeers list.
func (c HubConfig) IsPeerAllowed(name string) bool {
	for _, peer := range c.AllowedPeers {
		if peer.Name == name {
			return true
		}
	}
	return false
}

// ---- Channel allowlist computation -------------------------------------------

// effectiveSubscribeChannels returns the channels the hub should send to peerName,
// computed as the intersection of the peer's Subscribe request and the hub's
// Subscribe allowlist for that peer.
//
// Returns nil when the intersection is empty or requested is empty.
func effectiveSubscribeChannels(requested []string, peerName string, cfg HubConfig) []string {
	if len(requested) == 0 {
		return nil
	}
	for _, peer := range cfg.AllowedPeers {
		if peer.Name == peerName {
			if len(peer.Subscribe) == 0 {
				out := make([]string, len(requested))
				copy(out, requested)
				return out
			}
			return intersectChannels(requested, peer.Subscribe)
		}
	}
	return nil
}

// publishChannelsFor returns the Publish allowlist for peerName.
// An empty list means "accept any channels" (returns nil).
func publishChannelsFor(peerName string, cfg HubConfig) []string {
	for _, peer := range cfg.AllowedPeers {
		if peer.Name == peerName {
			return peer.Publish
		}
	}
	return nil
}

// intersectChannels returns elements of a that also appear in b.
// Returns nil when the result would be empty.
func intersectChannels(a, b []string) []string {
	if len(b) == 0 {
		return nil
	}
	set := make(map[string]bool, len(b))
	for _, s := range b {
		set[s] = true
	}
	var result []string
	for _, s := range a {
		if set[s] {
			result = append(result, s)
		}
	}
	return result
}

// validate checks that the HubConfig is internally consistent:
// every peer must have a non-empty Name, and peer names must be unique.
func (c HubConfig) validate() error {
	seen := make(map[string]bool, len(c.AllowedPeers))
	for i, peer := range c.AllowedPeers {
		if peer.Name == "" {
			return fmt.Errorf("federation: allowed_peers[%d]: name must not be empty", i)
		}
		if seen[peer.Name] {
			return fmt.Errorf("federation: allowed_peers: duplicate name %q", peer.Name)
		}
		seen[peer.Name] = true
	}
	return nil
}
