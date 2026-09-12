package federation

import (
	"github.com/wu/keyop-messenger/internal/storage"
)

// ReapOrphanedOutboundOffsets deletes client-side outbound publish offset files
// ("fedout-{hubAddr}.offset" under {dataDir}/subscribers/{channel}/) that do not
// correspond to any currently-configured hub.
//
// Clients build one outbound offset file per (hub, channel) and never remove
// them on Close, so a hub removed from the client configuration would otherwise
// leave its fedout-*.offset behind forever. Because the compactor includes
// fedout- files in its minimum-offset boundary, such an orphan permanently
// anchors compaction and leaks disk. The hub TTL sweep only matches the "fed-"
// prefix and only runs when a hub is enabled, so it does not cover this case.
//
// configuredHubAddrs is the set of hub addresses still present in the client
// configuration (empty when the client role is disabled, which orphans every
// fedout- file). Matching is done in sanitized-filename space because
// sanitizeForFilename is not reversible. Only the "fedout-" namespace is
// touched; inbound "fed-" files and ordinary subscriber offsets are left alone.
//
// It is intended to run once at startup, before clients are dialed — which is
// when a configuration change takes effect, since clients are built at New().
func ReapOrphanedOutboundOffsets(layout storage.Layout, configuredHubAddrs []string, log logger) {
	if layout.DataDir() == "" {
		return
	}

	expected := make(map[string]struct{}, len(configuredHubAddrs))
	for _, addr := range configuredHubAddrs {
		expected[storage.OffsetPrefixFedOut+sanitizeForFilename(addr)] = struct{}{}
	}

	swept, err := layout.SweepOffsets(storage.OffsetPrefixFedOut, func(f storage.OffsetFile) bool {
		_, configured := expected[f.ID]
		return configured
	})
	if err != nil {
		log.Error("federation: reap orphaned outbound offsets, list channels", "err", err)
		return
	}
	for _, r := range swept {
		if r.Err != nil {
			log.Error("federation: reap orphaned outbound offset, remove failed",
				"path", r.File.Path, "err", r.Err)
			continue
		}
		log.Info("federation: reaped orphaned outbound offset",
			"file", r.File.ID, "channel", r.File.Channel)
	}
}
