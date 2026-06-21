package messenger

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// intPtr returns a pointer to n. Used to set *int config fields in tests.
func intPtr(n int) *int { return &n }

// minValidConfig returns the smallest Config that passes Validate after ApplyDefaults.
func minValidConfig() Config {
	var c Config
	c.Storage.DataDir = "/var/keyop"
	c.ApplyDefaults()
	return c
}

// TestValidate_Valid confirms that a minimal config with only data_dir set passes
// after defaults are applied.
func TestValidate_Valid(t *testing.T) {
	c := minValidConfig()
	require.NoError(t, c.Validate())
}

// TestValidate_Errors covers every field-level validation rule.
func TestValidate_Errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantMsg string
	}{
		{
			name:    "missing data_dir",
			mutate:  func(c *Config) { c.Storage.DataDir = "" },
			wantMsg: "storage.data_dir is required",
		},
		{
			name:    "negative sync_interval_ms",
			mutate:  func(c *Config) { c.Storage.SyncIntervalMS = -1 },
			wantMsg: "storage.sync_interval_ms must be non-negative",
		},
		{
			name:    "negative max_retries",
			mutate:  func(c *Config) { c.Subscribers.MaxRetries = intPtr(-1) },
			wantMsg: "subscribers.max_retries must be non-negative",
		},
		{
			name: "hub enabled without listen_addr",
			mutate: func(c *Config) {
				c.Hub.Enabled = true
				c.Hub.ListenAddr = ""
			},
			wantMsg: "hub.listen_addr is required when hub.enabled is true",
		},
		{
			name: "client hub with empty addr",
			mutate: func(c *Config) {
				c.Client.Enabled = true
				c.Client.Hubs = []ClientHubRef{{Addr: ""}}
			},
			wantMsg: "client.hubs[0].addr is required",
		},
		{
			name: "tls cert without key",
			mutate: func(c *Config) {
				c.TLS.Cert = "/etc/certs/host.crt"
				c.TLS.Key = ""
			},
			wantMsg: "tls.key is required when tls.cert is set",
		},
		{
			name: "tls key without cert",
			mutate: func(c *Config) {
				c.TLS.Key = "/etc/certs/host.key"
				c.TLS.Cert = ""
			},
			wantMsg: "tls.cert is required when tls.key is set",
		},
		{
			name:    "reconnect jitter above 1",
			mutate:  func(c *Config) { c.Federation.ReconnectJitter = 1.5 },
			wantMsg: "federation.reconnect_jitter must be between 0 and 1",
		},
		{
			name:    "reconnect jitter below 0",
			mutate:  func(c *Config) { c.Federation.ReconnectJitter = -0.1 },
			wantMsg: "federation.reconnect_jitter must be between 0 and 1",
		},
		{
			name:    "zero seen_id_lru_size",
			mutate:  func(c *Config) { c.Dedup.SeenIDLRUSize = 0 },
			wantMsg: "dedup.seen_id_lru_size must be positive",
		},
		{
			name:    "zero audit max_size_mb",
			mutate:  func(c *Config) { c.Audit.MaxSizeMB = 0 },
			wantMsg: "audit.max_size_mb must be positive",
		},
		{
			name:    "zero audit max_files",
			mutate:  func(c *Config) { c.Audit.MaxFiles = 0 },
			wantMsg: "audit.max_files must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := minValidConfig()
			tt.mutate(&c)
			err := c.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}

// TestValidate_MultipleErrors confirms that all errors are returned together,
// not just the first one encountered.
func TestValidate_MultipleErrors(t *testing.T) {
	c := minValidConfig()
	c.Storage.DataDir = ""
	c.Hub.Enabled = true
	c.Hub.ListenAddr = ""

	err := c.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage.data_dir is required")
	assert.Contains(t, err.Error(), "hub.listen_addr is required")
}

// TestApplyDefaults confirms that defaults are applied to zero-valued fields
// and that explicitly set non-zero values are not overwritten.
func TestApplyDefaults(t *testing.T) {
	t.Run("zeros get defaults", func(t *testing.T) {
		var c Config
		c.ApplyDefaults()

		assert.Equal(t, 0, c.Storage.SyncIntervalMS)
		assert.Equal(t, 256, c.Storage.CompactionThresholdMB)
		require.NotNil(t, c.Subscribers.MaxRetries)
		assert.Equal(t, 5, *c.Subscribers.MaxRetries)
		assert.Equal(t, 30, c.TLS.ExpiryWarnDays)
		assert.Equal(t, 500, c.Federation.ReconnectBaseMS)
		assert.Equal(t, 60_000, c.Federation.ReconnectMaxMS)
		assert.Equal(t, 0.2, c.Federation.ReconnectJitter)
		assert.Equal(t, 10_000, c.Federation.SendBufferMessages)
		assert.Equal(t, 4*1024*1024, c.Federation.MaxBatchBytes)
		assert.Equal(t, 100_000, c.Dedup.SeenIDLRUSize)
		assert.Equal(t, 100, c.Audit.MaxSizeMB)
		assert.Equal(t, 10, c.Audit.MaxFiles)
	})

	t.Run("explicit values are not overwritten", func(t *testing.T) {
		c := Config{
			Storage: StorageConfig{
				SyncIntervalMS:        999,
				CompactionThresholdMB: 512,
			},
			Subscribers: SubscribersConfig{MaxRetries: intPtr(10)},
			TLS:         TLSConfig{ExpiryWarnDays: 60},
			Federation: FederationConfig{
				ReconnectBaseMS:    1000,
				ReconnectMaxMS:     30_000,
				ReconnectJitter:    0.1,
				SendBufferMessages: 5000,
				MaxBatchBytes:      32_768,
			},
			Dedup: DedupConfig{SeenIDLRUSize: 50_000},
			Audit: AuditConfig{MaxSizeMB: 200, MaxFiles: 5},
		}
		c.ApplyDefaults()

		assert.Equal(t, 999, c.Storage.SyncIntervalMS)
		assert.Equal(t, 512, c.Storage.CompactionThresholdMB)
		require.NotNil(t, c.Subscribers.MaxRetries)
		assert.Equal(t, 10, *c.Subscribers.MaxRetries)
		assert.Equal(t, 60, c.TLS.ExpiryWarnDays)
		assert.Equal(t, 1000, c.Federation.ReconnectBaseMS)
		assert.Equal(t, 30_000, c.Federation.ReconnectMaxMS)
		assert.Equal(t, 0.1, c.Federation.ReconnectJitter)
		assert.Equal(t, 5000, c.Federation.SendBufferMessages)
		assert.Equal(t, 32_768, c.Federation.MaxBatchBytes)
		assert.Equal(t, 50_000, c.Dedup.SeenIDLRUSize)
		assert.Equal(t, 200, c.Audit.MaxSizeMB)
		assert.Equal(t, 5, c.Audit.MaxFiles)
	})

	t.Run("MaxRetries zero is preserved, not defaulted", func(t *testing.T) {
		c := Config{Subscribers: SubscribersConfig{MaxRetries: intPtr(0)}}
		c.ApplyDefaults()
		require.NotNil(t, c.Subscribers.MaxRetries)
		assert.Equal(t, 0, *c.Subscribers.MaxRetries)
	})

}

// TestLoadConfig_RoundTrip writes a fully-populated config YAML to a temp file,
// loads it via LoadConfig, and asserts every field matches the original.
func TestLoadConfig_RoundTrip(t *testing.T) {
	const yaml = `
storage:
  data_dir: /var/keyop
  sync_interval_ms: 500
  max_channel_size_mb: 2048
  retention: 7d
  compaction_threshold_mb: 512
subscribers:
  max_retries: 10
hub:
  enabled: true
  listen_addr: "0.0.0.0:7741"
  allowed_peers:
    - name: client-a
    - name: client-b
client:
  enabled: true
  hubs:
    - addr: hub2.internal:7740
      subscribe:
        - alerts
        - events
      publish:
        - ack
tls:
  cert: /etc/certs/host.crt
  key:  /etc/certs/host.key
  ca:   /etc/certs/ca.crt
  expiry_warn_days: 60
federation:
  reconnect_base_ms: 1000
  reconnect_max_ms: 30000
  reconnect_jitter: 0.1
  send_buffer_messages: 5000
  max_batch_bytes: 32768
dedup:
  seen_id_lru_size: 50000
audit:
  max_size_mb: 200
  max_files: 5
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	assert.Equal(t, "/var/keyop", cfg.Storage.DataDir)
	assert.Equal(t, 500, cfg.Storage.SyncIntervalMS)
	assert.Equal(t, 2048, cfg.Storage.MaxChannelSizeMB)
	assert.Equal(t, 7*24*time.Hour, cfg.Storage.RetentionAge.Duration)
	assert.Equal(t, 512, cfg.Storage.CompactionThresholdMB)
	require.NotNil(t, cfg.Subscribers.MaxRetries)
	assert.Equal(t, 10, *cfg.Subscribers.MaxRetries)
	assert.True(t, cfg.Hub.Enabled)
	assert.Equal(t, "0.0.0.0:7741", cfg.Hub.ListenAddr)
	require.Len(t, cfg.Hub.AllowedPeers, 2)
	assert.Equal(t, "client-a", cfg.Hub.AllowedPeers[0].Name)
	assert.Equal(t, "client-b", cfg.Hub.AllowedPeers[1].Name)
	assert.True(t, cfg.Client.Enabled)
	require.Len(t, cfg.Client.Hubs, 1)
	assert.Equal(t, "hub2.internal:7740", cfg.Client.Hubs[0].Addr)
	assert.Equal(t, []string{"alerts", "events"}, cfg.Client.Hubs[0].Subscribe)
	assert.Equal(t, []string{"ack"}, cfg.Client.Hubs[0].Publish)
	assert.Equal(t, "/etc/certs/host.crt", cfg.TLS.Cert)
	assert.Equal(t, "/etc/certs/host.key", cfg.TLS.Key)
	assert.Equal(t, "/etc/certs/ca.crt", cfg.TLS.CA)
	assert.Equal(t, 60, cfg.TLS.ExpiryWarnDays)
	assert.Equal(t, 1000, cfg.Federation.ReconnectBaseMS)
	assert.Equal(t, 30_000, cfg.Federation.ReconnectMaxMS)
	assert.Equal(t, 0.1, cfg.Federation.ReconnectJitter)
	assert.Equal(t, 5000, cfg.Federation.SendBufferMessages)
	assert.Equal(t, 32_768, cfg.Federation.MaxBatchBytes)
	assert.Equal(t, 50_000, cfg.Dedup.SeenIDLRUSize)
	assert.Equal(t, 200, cfg.Audit.MaxSizeMB)
	assert.Equal(t, 5, cfg.Audit.MaxFiles)
}

// TestLoadConfig_MaxRetriesZero confirms that max_retries: 0 in YAML is preserved
// and not replaced by the default of 5.
func TestLoadConfig_MaxRetriesZero(t *testing.T) {
	const yaml = `
name: test-host
storage:
  data_dir: /var/keyop
subscribers:
  max_retries: 0
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.NotNil(t, cfg.Subscribers.MaxRetries)
	assert.Equal(t, 0, *cfg.Subscribers.MaxRetries)
}

// TestLoadConfig_FileNotFound verifies that a missing file returns a wrapped os.ErrNotExist.
func TestLoadConfig_FileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/config.yaml")
	require.Error(t, err)
	assert.True(t, errors.Is(err, os.ErrNotExist), "expected os.ErrNotExist, got: %v", err)
}

// TestLoadConfig_InvalidYAML verifies that malformed YAML returns a decode error.
func TestLoadConfig_InvalidYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(":{invalid yaml"), 0600))

	_, err := LoadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode config")
}

// TestLoadConfig_ValidationFailure verifies that a config missing required fields
// returns a validation error from LoadConfig.
func TestLoadConfig_ValidationFailure(t *testing.T) {
	// data_dir is absent; defaults will not supply it.
	const yaml = `
name: test-host
storage: {}
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0600))

	_, err := LoadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage.data_dir is required")
}

// ---- Duration.UnmarshalYAML -------------------------------------------------

// TestDuration_UnmarshalYAML verifies that Duration correctly parses Go duration
// strings when decoded from YAML.
func TestDuration_UnmarshalYAML(t *testing.T) {
	cases := []struct {
		input string
		want  time.Duration
	}{
		{"1h", time.Hour},
		{"30m", 30 * time.Minute},
		{"168h", 168 * time.Hour},
		{"500ms", 500 * time.Millisecond},
		{"0s", 0},
		{"7d", 7 * 24 * time.Hour},
		{"1d", 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"1.5d", 36 * time.Hour},
		{"1d12h", 36 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			var d Duration
			require.NoError(t, yaml.Unmarshal([]byte(`"`+tc.input+`"`), &d))
			assert.Equal(t, tc.want, d.Duration)
		})
	}
}

// TestDuration_UnmarshalYAML_Invalid verifies that an unparseable duration
// string returns a wrapped error that includes "invalid duration".
func TestDuration_UnmarshalYAML_Invalid(t *testing.T) {
	var d Duration
	err := yaml.Unmarshal([]byte(`"not-a-duration"`), &d)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid duration")
}

// TestLoadConfig_WithFedClientOffsetTTL verifies that hub.fed_client_offset_ttl
// is parsed via Duration.UnmarshalYAML and reflects the correct value.
func TestLoadConfig_WithFedClientOffsetTTL(t *testing.T) {
	const input = `
name: test-host
storage:
  data_dir: /var/keyop
hub:
  fed_client_offset_ttl: "336h"
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(input), 0600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, 336*time.Hour, cfg.Hub.FedClientOffsetTTL.Duration)
}

// TestLoadConfig_InvalidFedClientOffsetTTL verifies that an unparseable
// fed_client_offset_ttl causes LoadConfig to return a decode error.
func TestLoadConfig_InvalidFedClientOffsetTTL(t *testing.T) {
	const input = `
name: test-host
storage:
  data_dir: /var/keyop
hub:
  fed_client_offset_ttl: "not-a-duration"
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(input), 0600))

	_, err := LoadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode config")
}

// TestLoadConfig_AllowedPeers_SubscribePublish verifies that per-peer subscribe
// and publish channel lists round-trip correctly through YAML.
func TestLoadConfig_AllowedPeers_SubscribePublish(t *testing.T) {
	const input = `
name: test-host
storage:
  data_dir: /var/keyop
hub:
  enabled: true
  listen_addr: "0.0.0.0:7740"
  allowed_peers:
    - name: client-a
      subscribe: [orders, events]
      publish:   [commands]
    - name: client-b
      subscribe: [alerts]
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(input), 0600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)

	require.Len(t, cfg.Hub.AllowedPeers, 2)

	peerA := cfg.Hub.AllowedPeers[0]
	assert.Equal(t, "client-a", peerA.Name)
	assert.Equal(t, []string{"orders", "events"}, peerA.Subscribe)
	assert.Equal(t, []string{"commands"}, peerA.Publish)

	peerB := cfg.Hub.AllowedPeers[1]
	assert.Equal(t, "client-b", peerB.Name)
	assert.Equal(t, []string{"alerts"}, peerB.Subscribe)
	assert.Empty(t, peerB.Publish)
}

// TestLoadConfig_StorageOffsetFlushIntervalMS verifies that
// storage.offset_flush_interval_ms round-trips through YAML.
func TestLoadConfig_StorageOffsetFlushIntervalMS(t *testing.T) {
	const input = `
name: test-host
storage:
  data_dir: /var/keyop
  offset_flush_interval_ms: 250
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(input), 0600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, 250, cfg.Storage.OffsetFlushIntervalMS)
}
