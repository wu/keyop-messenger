package messenger

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// SyncPolicy controls when channel file writes are flushed to stable storage.
type SyncPolicy string

const (
	// SyncPolicyNone lets the OS flush at its own discretion.
	// Publish() returns after write() succeeds. Fastest; data in OS page cache only.
	SyncPolicyNone SyncPolicy = "none"

	// SyncPolicyPeriodic flushes via a background fsync ticker.
	// Publish() returns after write() succeeds; fsync runs on a separate timer.
	// Durable against application crashes; not against OS crashes between intervals.
	SyncPolicyPeriodic SyncPolicy = "periodic"

	// SyncPolicyAlways fsyncs after every write before Publish() returns.
	// Slowest; durable against OS crashes and power failure.
	SyncPolicyAlways SyncPolicy = "always"
)

// StorageConfig controls the on-disk message store.
type StorageConfig struct {
	// DataDir is the root directory for channel files, subscriber offsets, and the audit log.
	// Required.
	DataDir string `yaml:"data_dir"`

	// SyncPolicy controls when writes are flushed to stable storage.
	// Default: "periodic".
	SyncPolicy SyncPolicy `yaml:"sync_policy"`

	// SyncIntervalMS is the fsync interval in milliseconds when SyncPolicy is "periodic".
	// Default: 200.
	SyncIntervalMS int `yaml:"sync_interval_ms"`

	// MaxSubscriberLagMB triggers a warning log when a subscriber's unread backlog
	// exceeds this many megabytes. Default: 512.
	MaxSubscriberLagMB int `yaml:"max_subscriber_lag_mb"`

	// CompactionThresholdMB triggers file rotation when the consumed (already-read-by-all-
	// subscribers) portion of a channel file exceeds this size. Default: 256.
	CompactionThresholdMB int `yaml:"compaction_threshold_mb"`
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

// AllowedClient is an instance that is permitted to connect to this hub as a client.
type AllowedClient struct {
	// Name is the instance name embedded in the client's TLS certificate CN.
	Name string `yaml:"name"`
}

// PeerHubConfig describes a peer hub connection and the channels forwarded over it.
type PeerHubConfig struct {
	// Addr is the host:port of the peer hub. The hostname must match the peer cert's CN.
	Addr string `yaml:"addr"`

	// Forward lists the channels this hub will send to the peer hub (exact names only).
	Forward []string `yaml:"forward"`

	// Receive lists the channels this hub will accept from the peer hub (exact names only).
	// Messages on unlisted channels are discarded and audit-logged as policy violations.
	Receive []string `yaml:"receive"`
}

// HubConfig controls the hub listener and its federation policy.
type HubConfig struct {
	// Enabled starts a WebSocket listener on ListenAddr when true. Default: false.
	Enabled bool `yaml:"enabled"`

	// ListenAddr is the address to listen on, e.g. "0.0.0.0:7740".
	// Required when Enabled is true.
	ListenAddr string `yaml:"listen_addr"`

	// AllowedClients is the explicit list of client instance names permitted to connect.
	// Connections from instances not in this list are rejected after the mTLS handshake.
	AllowedClients []AllowedClient `yaml:"allowed_clients"`

	// PeerHubs lists peer hubs to connect to and the forwarding policy for each.
	PeerHubs []PeerHubConfig `yaml:"peer_hubs"`
}

// ClientHubRef is a hub address a client instance dials.
type ClientHubRef struct {
	// Addr is the host:port of the hub to dial.
	Addr string `yaml:"addr"`
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

	// MinVersion is the minimum TLS version. Default: "1.3".
	MinVersion string `yaml:"min_version"`

	// ExpiryWarnDays triggers a warning log when the instance cert or CA cert expires
	// within this many days. Default: 30.
	ExpiryWarnDays int `yaml:"expiry_warn_days"`
}

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
	// Default: 65536.
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
type Config struct {
	// Name is the human-readable identifier for this instance.
	// Defaults to the OS hostname. Use "hostname:port" when multiple instances share a host.
	Name string `yaml:"name"`

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
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %q: %w", path, err)
	}
	defer f.Close()

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
	if c.Name == "" {
		if h, err := os.Hostname(); err == nil {
			c.Name = h
		}
	}

	if c.Storage.SyncPolicy == "" {
		c.Storage.SyncPolicy = SyncPolicyPeriodic
	}
	if c.Storage.SyncIntervalMS == 0 {
		c.Storage.SyncIntervalMS = 200
	}
	if c.Storage.MaxSubscriberLagMB == 0 {
		c.Storage.MaxSubscriberLagMB = 512
	}
	if c.Storage.CompactionThresholdMB == 0 {
		c.Storage.CompactionThresholdMB = 256
	}

	if c.Subscribers.MaxRetries == nil {
		n := 5
		c.Subscribers.MaxRetries = &n
	}

	if c.TLS.MinVersion == "" {
		c.TLS.MinVersion = "1.3"
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
		c.Federation.MaxBatchBytes = 65_536
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

	if c.Name == "" {
		errs = append(errs, errors.New("name is required (set explicitly or ensure os.Hostname() succeeds)"))
	}

	if c.Storage.DataDir == "" {
		errs = append(errs, errors.New("storage.data_dir is required"))
	}

	switch c.Storage.SyncPolicy {
	case SyncPolicyNone, SyncPolicyPeriodic, SyncPolicyAlways:
		// valid
	default:
		errs = append(errs, fmt.Errorf(
			"storage.sync_policy %q is invalid: must be %q, %q, or %q",
			c.Storage.SyncPolicy, SyncPolicyNone, SyncPolicyPeriodic, SyncPolicyAlways,
		))
	}

	if c.Storage.SyncPolicy == SyncPolicyPeriodic && c.Storage.SyncIntervalMS <= 0 {
		errs = append(errs, errors.New("storage.sync_interval_ms must be positive when sync_policy is \"periodic\""))
	}

	if c.Subscribers.MaxRetries != nil && *c.Subscribers.MaxRetries < 0 {
		errs = append(errs, errors.New("subscribers.max_retries must be non-negative"))
	}

	if c.Hub.Enabled && c.Hub.ListenAddr == "" {
		errs = append(errs, errors.New("hub.listen_addr is required when hub.enabled is true"))
	}

	for i, peer := range c.Hub.PeerHubs {
		if peer.Addr == "" {
			errs = append(errs, fmt.Errorf("hub.peer_hubs[%d].addr is required", i))
		}
	}

	if c.Client.Enabled {
		for i, h := range c.Client.Hubs {
			if h.Addr == "" {
				errs = append(errs, fmt.Errorf("client.hubs[%d].addr is required", i))
			}
		}
	}

	switch c.TLS.MinVersion {
	case "1.2", "1.3":
		// valid
	default:
		errs = append(errs, fmt.Errorf(
			"tls.min_version %q is invalid: must be \"1.2\" or \"1.3\"",
			c.TLS.MinVersion,
		))
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
