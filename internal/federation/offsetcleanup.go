package federation

import (
	"os"
	"path/filepath"
	"strings"
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
func ReapOrphanedOutboundOffsets(dataDir string, configuredHubAddrs []string, log logger) {
	if dataDir == "" {
		return
	}

	expected := make(map[string]struct{}, len(configuredHubAddrs))
	for _, addr := range configuredHubAddrs {
		expected["fedout-"+sanitizeForFilename(addr)+".offset"] = struct{}{}
	}

	subsDir := filepath.Join(dataDir, "subscribers")
	entries, err := os.ReadDir(subsDir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Error("federation: reap orphaned outbound offsets, read subscribers dir", "err", err)
		}
		return
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		channelDir := filepath.Join(subsDir, e.Name())
		files, err := os.ReadDir(channelDir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasPrefix(f.Name(), "fedout-") || !strings.HasSuffix(f.Name(), ".offset") {
				continue
			}
			if _, ok := expected[f.Name()]; ok {
				continue
			}
			path := filepath.Join(channelDir, f.Name())
			if rmErr := os.Remove(path); rmErr == nil {
				log.Info("federation: reaped orphaned outbound offset",
					"file", f.Name(), "channel", e.Name())
			} else {
				log.Error("federation: reap orphaned outbound offset, remove failed",
					"path", path, "err", rmErr)
			}
		}
	}
}
