// Package messenger implements a file-based pub-sub system.
package messenger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from a YAML string. It accepts Go
// duration strings (e.g. "168h", "30m", "500ms") plus a day unit "d" that Go's
// time.ParseDuration lacks, including compound values: "7d", "1.5d", "1d12h".
type Duration struct{ time.Duration }

// UnmarshalYAML implements yaml.Unmarshaler so Duration fields accept Go duration
// strings extended with a day unit.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	dur, err := parseDuration(value.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value.Value, err)
	}
	d.Duration = dur
	return nil
}

// parseDuration is time.ParseDuration extended with a day unit "d". A leading
// "<n>d" component (n may be fractional) is converted to hours and added to any
// standard duration remainder, so "7d", "1.5d", and "1d12h" all parse. Strings
// without a day component fall through to time.ParseDuration unchanged.
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	i := strings.IndexByte(s, 'd')
	if i < 0 {
		return time.ParseDuration(s)
	}
	days, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, fmt.Errorf("invalid day count %q", s[:i])
	}
	total := time.Duration(days * 24 * float64(time.Hour))
	if rest := s[i+1:]; rest != "" {
		restDur, err := time.ParseDuration(rest)
		if err != nil {
			return 0, err
		}
		total += restDur
	}
	return total, nil
}

// StorageConfig controls the on-disk message store.
type StorageConfig struct {
	// DataDir is the root directory for channel files, subscriber offsets, and the audit log.
	// Required.
	DataDir string `yaml:"data_dir"`

	// SyncIntervalMS controls when writes are flushed to stable storage.
	// 0 fsyncs after every write (default, strictest durability, slowest).
	// > 0 fsyncs periodically at this interval in milliseconds (batched, faster).
	SyncIntervalMS int `yaml:"sync_interval_ms"`

	// CompactionThresholdMB triggers file rotation when the consumed (already-read-by-all-
	// subscribers) portion of a channel file exceeds this size. Default: 256.
	CompactionThresholdMB int `yaml:"compaction_threshold_mb"`

	// DeadLetterCompactionThresholdMB is the segment roll size for dead-letter
	// channels ("{channel}.dead-letter"), the dead-letter counterpart of
	// CompactionThresholdMB. It is intentionally smaller so a dead-letter
	// channel's active segment seals sooner, letting age-based retention (see
	// RetentionAge and the default dead-letter retention) actually reclaim it on
	// a low-volume channel instead of holding everything in one never-rolled
	// active segment. Default: 64.
	DeadLetterCompactionThresholdMB int `yaml:"dead_letter_compaction_threshold_mb"`

	// MaxChannelSizeMB caps the on-disk size of a single channel's retained segment
	// files. When the total exceeds this, the compactor force-deletes the oldest
	// sealed segments — including ones a lagging subscriber has not yet consumed —
	// to bound disk usage. The active (currently-written) segment is never deleted,
	// so the effective floor is one segment (keep CompactionThresholdMB small to
	// keep that floor low). A subscriber whose offset is undercut fast-forwards past
	// the dropped messages. 0 (default) disables the size cap.
	MaxChannelSizeMB int `yaml:"max_channel_size_mb"`

	// RetentionAge caps how long messages are retained. The compactor force-deletes
	// sealed segments whose most recent write is older than this, even if a
	// subscriber has not consumed them. The active segment is never deleted.
	// 0 (default) disables age-based retention. Eviction occurs when EITHER
	// MaxChannelSizeMB or RetentionAge is exceeded. Accepts a day unit, e.g.
	// "7d" or "168h" for one week.
	RetentionAge Duration `yaml:"retention"`

	// OffsetFlushIntervalMS is the minimum time between subscriber offset file
	// flushes (fsync + atomic rename). 0 flushes after every delivered message,
	// which is the strictest at-least-once guarantee but slowest on high-latency
	// storage. Values > 0 batch flushes for higher throughput; on a crash the
	// subscriber may replay up to this many milliseconds of already-delivered
	// messages. Default: 0 (flush every message).
	OffsetFlushIntervalMS int `yaml:"offset_flush_interval_ms"`
}

// SubscribersConfig controls subscriber delivery behaviour.
type SubscribersConfig struct {
	// MaxRetries is the number of times a failing handler is retried before the message
	// is routed to the dead-letter channel. Default: 5.
	//
	// Use a pointer so that an explicit value of 0 (fail immediately to the dead-letter
	// channel) is distinguishable from "not set" when loading from YAML. A nil value
	// is replaced with the default of 5 by [Config.ApplyDefaults].
	MaxRetries *int `yaml:"max_retries"`
}

// AllowedPeer is an instance that is permitted to connect to this hub.
type AllowedPeer struct {
	// Name is the instance name embedded in the peer's TLS certificate CN.
	Name string `yaml:"name"`
	// Subscribe lists the channels this peer is permitted to receive (from hub to peer).
	// An empty list means the peer may subscribe to any channel.
	Subscribe []string `yaml:"subscribe"`
	// Publish lists the channels this peer is permitted to send to this hub (from peer to hub).
	// An empty list means the peer may publish to any channel.
	Publish []string `yaml:"publish"`
}

// HubConfig controls the hub listener and its federation policy.
type HubConfig struct {
	// Enabled starts a WebSocket listener on ListenAddr when true. Default: false.
	Enabled bool `yaml:"enabled"`

	// ListenAddr is the address to listen on, e.g. "0.0.0.0:7740".
	// Required when Enabled is true.
	ListenAddr string `yaml:"listen_addr"`

	// AllowedPeers is the explicit list of peer instance names permitted to connect.
	// Connections from instances not in this list are rejected after the mTLS handshake.
	AllowedPeers []AllowedPeer `yaml:"allowed_peers"`

	// FedClientOffsetTTL is how long a disconnected federation client's offset files
	// are retained before the hub deletes them. Offset files block compaction, so
	// stale files from clients that never reconnect must be cleaned up.
	// Default: 168h (1 week). Set to 0 to disable the TTL sweep entirely.
	FedClientOffsetTTL Duration `yaml:"fed_client_offset_ttl"`
}

// ClientHubRef is a hub address a client instance dials.
type ClientHubRef struct {
	// Addr is the host:port of the hub to dial.
	Addr string `yaml:"addr"`
	// Subscribe lists the channels to request from the hub (inbound to this instance).
	// The hub may deliver a subset based on its access control policy.
	Subscribe []string `yaml:"subscribe"`
	// Publish lists the channels this instance will send to the hub (outbound from this instance).
	// An empty list means this client will not publish any messages to the hub.
	// The hub may further restrict a subset based on its own receive policy.
	Publish []string `yaml:"publish"`
}

// ClientConfig controls outbound hub connections.
type ClientConfig struct {
	// Enabled dials the listed hubs on startup when true. Default: false.
	Enabled bool `yaml:"enabled"`

	// Hubs is the list of hub addresses to connect to.
	Hubs []ClientHubRef `yaml:"hubs"`
}

// TLSConfig holds paths to the instance's TLS credentials.
type TLSConfig struct {
	// Cert is the path to the instance's PEM-encoded certificate.
	Cert string `yaml:"cert"`

	// Key is the path to the instance's PEM-encoded private key.
	Key string `yaml:"key"`

	// CA is the path to the PEM-encoded CA certificate used to verify peers.
	CA string `yaml:"ca"`

	// ExpiryWarnDays triggers a warning log when the instance cert or CA cert expires
	// within this many days. Default: 30.
	ExpiryWarnDays int `yaml:"expiry_warn_days"`
}

// TLS 1.3 is hardcoded as the minimum protocol version for federation
// connections (see tlsutil.BuildTLSConfig). It is not configurable: this is a
// closed federation mesh, and there is no operational reason to allow
// downgrade to older TLS versions.

// FederationConfig controls WebSocket reconnection and message batching.
type FederationConfig struct {
	// ReconnectBaseMS is the initial reconnection backoff delay in milliseconds. Default: 500.
	ReconnectBaseMS int `yaml:"reconnect_base_ms"`

	// ReconnectMaxMS is the maximum reconnection backoff delay in milliseconds. Default: 60000.
	ReconnectMaxMS int `yaml:"reconnect_max_ms"`

	// ReconnectJitter is the fractional jitter applied to the backoff delay (0–1). Default: 0.2.
	ReconnectJitter float64 `yaml:"reconnect_jitter"`

	// SendBufferMessages is the maximum number of unacknowledged outbound messages buffered
	// per peer hub during a disconnection. Messages beyond this limit are dropped with a
	// warning. Default: 10000.
	SendBufferMessages int `yaml:"send_buffer_messages"`

	// MaxBatchBytes is the maximum size of a single WebSocket frame payload in bytes.
	// Default: 4194304 (4 MiB). Set to 0 to disable the limit.
	MaxBatchBytes int `yaml:"max_batch_bytes"`
}

// DedupConfig controls the in-memory seen-ID deduplication set.
type DedupConfig struct {
	// SeenIDLRUSize is the maximum number of message IDs held in the LRU dedup cache.
	// Default: 100000.
	SeenIDLRUSize int `yaml:"seen_id_lru_size"`
}

// AuditConfig controls audit log rotation.
type AuditConfig struct {
	// MaxSizeMB rotates audit.jsonl when it reaches this size. Default: 100.
	MaxSizeMB int `yaml:"max_size_mb"`

	// MaxFiles is the number of rotated audit files to retain. Default: 10.
	MaxFiles int `yaml:"max_files"`
}

// Config is the top-level configuration for a Messenger instance.
// Load it from YAML with [LoadConfig] or construct it programmatically.
//
// Instance identity is not configurable: it is derived from the local TLS
// certificate's Common Name. See [New] and [tlsutil.ExtractLocalCN].
type Config struct {
	Storage     StorageConfig     `yaml:"storage"`
	Subscribers SubscribersConfig `yaml:"subscribers"`
	Hub         HubConfig         `yaml:"hub"`
	Client      ClientConfig      `yaml:"client"`
	TLS         TLSConfig         `yaml:"tls"`
	Federation  FederationConfig  `yaml:"federation"`
	Dedup       DedupConfig       `yaml:"dedup"`
	Audit       AuditConfig       `yaml:"audit"`
}

// LoadConfig reads a YAML config file, applies defaults, and validates the result.
func LoadConfig(path string) (*Config, error) {
	//nolint:gosec // G304: loading config from known path (not user input)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config %q: %w", path, err)
	}

	cfg.ApplyDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %q: %w", path, err)
	}

	return &cfg, nil
}

// ApplyDefaults fills in zero-valued fields with their documented defaults.
// It is called automatically by [LoadConfig]. When constructing a Config
// programmatically, call ApplyDefaults before passing it to [New].
func (c *Config) ApplyDefaults() {
	if c.Storage.CompactionThresholdMB == 0 {
		c.Storage.CompactionThresholdMB = 256
	}

	if c.Storage.DeadLetterCompactionThresholdMB == 0 {
		c.Storage.DeadLetterCompactionThresholdMB = 64
	}

	if c.Subscribers.MaxRetries == nil {
		n := 5
		c.Subscribers.MaxRetries = &n
	}

	if c.TLS.ExpiryWarnDays == 0 {
		c.TLS.ExpiryWarnDays = 30
	}

	if c.Federation.ReconnectBaseMS == 0 {
		c.Federation.ReconnectBaseMS = 500
	}
	if c.Federation.ReconnectMaxMS == 0 {
		c.Federation.ReconnectMaxMS = 60_000
	}
	if c.Federation.ReconnectJitter == 0 {
		c.Federation.ReconnectJitter = 0.2
	}
	if c.Federation.SendBufferMessages == 0 {
		c.Federation.SendBufferMessages = 10_000
	}
	if c.Federation.MaxBatchBytes == 0 {
		c.Federation.MaxBatchBytes = 4 * 1024 * 1024 // 4 MiB
	}

	if c.Dedup.SeenIDLRUSize == 0 {
		c.Dedup.SeenIDLRUSize = 100_000
	}

	if c.Audit.MaxSizeMB == 0 {
		c.Audit.MaxSizeMB = 100
	}
	if c.Audit.MaxFiles == 0 {
		c.Audit.MaxFiles = 10
	}

	if c.Hub.FedClientOffsetTTL.Duration == 0 {
		c.Hub.FedClientOffsetTTL = Duration{168 * time.Hour}
	}
}

// EnsureDirectories creates the directory layout required by a Messenger under
// cfg.Storage.DataDir. It is called automatically by New; callers rarely need
// it directly.
func EnsureDirectories(cfg *Config) error {
	dirs := []string{
		filepath.Join(cfg.Storage.DataDir, "channels"),
		filepath.Join(cfg.Storage.DataDir, "subscribers"),
		filepath.Join(cfg.Storage.DataDir, "audit"),
	}
	for _, d := range dirs {
		//nolint:gosec // G301: 0o755 is appropriate for shared data directories
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("create directory %q: %w", d, err)
		}
	}
	return nil
}

// Validate returns an error describing all configuration problems found.
// It expects [ApplyDefaults] to have been called first.
func (c *Config) Validate() error {
	var errs []error

	if c.Storage.DataDir == "" {
		errs = append(errs, errors.New("storage.data_dir is required"))
	}

	if c.Storage.SyncIntervalMS < 0 {
		errs = append(errs, errors.New("storage.sync_interval_ms must be non-negative"))
	}

	if c.Subscribers.MaxRetries != nil && *c.Subscribers.MaxRetries < 0 {
		errs = append(errs, errors.New("subscribers.max_retries must be non-negative"))
	}

	if c.Hub.Enabled && c.Hub.ListenAddr == "" {
		errs = append(errs, errors.New("hub.listen_addr is required when hub.enabled is true"))
	}

	if c.Client.Enabled {
		for i, h := range c.Client.Hubs {
			if h.Addr == "" {
				errs = append(errs, fmt.Errorf("client.hubs[%d].addr is required", i))
			}
		}
	}

	if c.TLS.Cert != "" && c.TLS.Key == "" {
		errs = append(errs, errors.New("tls.key is required when tls.cert is set"))
	}
	if c.TLS.Key != "" && c.TLS.Cert == "" {
		errs = append(errs, errors.New("tls.cert is required when tls.key is set"))
	}

	if c.Federation.ReconnectJitter < 0 || c.Federation.ReconnectJitter > 1 {
		errs = append(errs, errors.New("federation.reconnect_jitter must be between 0 and 1"))
	}

	if c.Dedup.SeenIDLRUSize <= 0 {
		errs = append(errs, errors.New("dedup.seen_id_lru_size must be positive"))
	}

	if c.Audit.MaxSizeMB <= 0 {
		errs = append(errs, errors.New("audit.max_size_mb must be positive"))
	}
	if c.Audit.MaxFiles <= 0 {
		errs = append(errs, errors.New("audit.max_files must be positive"))
	}

	return errors.Join(errs...)
}
