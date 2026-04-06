package federation

import (
	"fmt"
	"net"
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
	// AllowChannels lists the channels this client is permitted to subscribe to.
	// An empty list means the client may subscribe to any channel.
	AllowChannels []string `yaml:"allow_channels"`
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

// effectiveForwardChannels returns the channels the hub should send to peerName,
// computed as the intersection of the peer's Subscribe request and the hub's
// allowlist for that peer.
//
//   - For peer hubs (in PeerHubs): PeerHubConfig.Forward is the allowlist.
//   - For allowed clients (in AllowedClients): AllowedClient.AllowChannels is the
//     allowlist; an empty AllowChannels means "permit all subscribed channels".
//
// Returns nil when the intersection is empty or requested is empty.
func effectiveForwardChannels(requested []string, peerName string, cfg HubConfig) []string {
	if len(requested) == 0 {
		return nil
	}
	for _, ph := range cfg.PeerHubs {
		if hostnameMatch(ph.Addr, peerName) {
			return intersectChannels(requested, ph.Forward)
		}
	}
	for _, ac := range cfg.AllowedClients {
		if ac.Name == peerName {
			if len(ac.AllowChannels) == 0 {
				out := make([]string, len(requested))
				copy(out, requested)
				return out
			}
			return intersectChannels(requested, ac.AllowChannels)
		}
	}
	return nil
}

// receiveChannelsFor returns the Receive allowlist for peerName if it is a
// configured peer hub, otherwise nil (accept all inbound channels).
func receiveChannelsFor(peerName string, cfg HubConfig) []string {
	for _, ph := range cfg.PeerHubs {
		if hostnameMatch(ph.Addr, peerName) {
			return ph.Receive
		}
	}
	return nil
}

// hostnameMatch reports whether addr (a "host:port" or bare hostname) matches name.
func hostnameMatch(addr, name string) bool {
	if addr == name {
		return true
	}
	// Extract hostname from addr and compare.
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h == name
	}
	return false
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
