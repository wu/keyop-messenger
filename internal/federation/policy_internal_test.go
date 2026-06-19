package federation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// These tests cover the unexported channel-allowlist computation helpers, which
// implement the security-relevant "effective set = subscribe ∩ allowlist" rule.
// They live in package federation (not federation_test) because the functions
// are unexported.

// ---- effectiveSubscribeChannels --------------------------------------------

func TestEffectiveSubscribeChannels(t *testing.T) {
	cfg := HubConfig{
		AllowedPeers: []AllowedPeer{
			{Name: "restricted", Subscribe: []string{"orders", "billing"}},
			{Name: "unrestricted"}, // empty Subscribe = allow whatever is requested
		},
	}

	tests := []struct {
		name      string
		requested []string
		peer      string
		want      []string
	}{
		{
			name:      "empty request returns nil",
			requested: nil,
			peer:      "restricted",
			want:      nil,
		},
		{
			name:      "unknown peer returns nil",
			requested: []string{"orders"},
			peer:      "ghost",
			want:      nil,
		},
		{
			name:      "empty allowlist passes requested through",
			requested: []string{"orders", "shipping"},
			peer:      "unrestricted",
			want:      []string{"orders", "shipping"},
		},
		{
			name:      "allowlist intersects requested",
			requested: []string{"orders", "shipping", "billing"},
			peer:      "restricted",
			want:      []string{"orders", "billing"},
		},
		{
			name:      "no overlap returns nil",
			requested: []string{"shipping"},
			peer:      "restricted",
			want:      nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, effectiveSubscribeChannels(tc.requested, tc.peer, cfg))
		})
	}
}

// TestEffectiveSubscribeChannels_EmptyAllowlistReturnsCopy verifies the empty
// allowlist branch returns a defensive copy: mutating the result must not alter
// the caller's requested slice.
func TestEffectiveSubscribeChannels_EmptyAllowlistReturnsCopy(t *testing.T) {
	cfg := HubConfig{AllowedPeers: []AllowedPeer{{Name: "p"}}}
	requested := []string{"orders", "billing"}

	got := effectiveSubscribeChannels(requested, "p", cfg)
	assert.Equal(t, requested, got)

	got[0] = "mutated"
	assert.Equal(t, "orders", requested[0], "result must be a copy, not alias the input")
}

// ---- publishChannelsFor -----------------------------------------------------

func TestPublishChannelsFor(t *testing.T) {
	cfg := HubConfig{
		AllowedPeers: []AllowedPeer{
			{Name: "limited", Publish: []string{"metrics"}},
			{Name: "open"}, // empty Publish = accept any channel
		},
	}

	assert.Equal(t, []string{"metrics"}, publishChannelsFor("limited", cfg))
	assert.Nil(t, publishChannelsFor("open", cfg), "empty allowlist means accept any -> nil")
	assert.Nil(t, publishChannelsFor("unknown", cfg), "unknown peer -> nil")
}

// ---- intersectChannels ------------------------------------------------------

func TestIntersectChannels(t *testing.T) {
	tests := []struct {
		name string
		a    []string
		b    []string
		want []string
	}{
		{
			name: "empty b returns nil",
			a:    []string{"x", "y"},
			b:    nil,
			want: nil,
		},
		{
			name: "no overlap returns nil",
			a:    []string{"x"},
			b:    []string{"y"},
			want: nil,
		},
		{
			name: "partial overlap preserves order of a",
			a:    []string{"c", "a", "b"},
			b:    []string{"a", "b", "c"},
			want: []string{"c", "a", "b"},
		},
		{
			name: "filters to common elements",
			a:    []string{"a", "x", "b", "y"},
			b:    []string{"a", "b"},
			want: []string{"a", "b"},
		},
		{
			name: "duplicates in a are kept",
			a:    []string{"a", "a", "z"},
			b:    []string{"a"},
			want: []string{"a", "a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, intersectChannels(tc.a, tc.b))
		})
	}
}
