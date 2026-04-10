package messenger

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// intPtr returns a pointer to n. Used to set *int config fields in tests.
func intPtr(n int) *int { return &n }

// minValidConfig returns the smallest Config that passes Validate after ApplyDefaults.
// Name is set explicitly as a fallback in case os.Hostname() fails on the test machine.
func minValidConfig() Config {
	var c Config
	c.Storage.DataDir = "/var/keyop"
	c.ApplyDefaults()
	if c.Name == "" {
		c.Name = "test-host"
	}
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
			name:    "empty name",
			mutate:  func(c *Config) { c.Name = "" },
			wantMsg: "name is required",
		},
		{
			name:    "missing data_dir",
			mutate:  func(c *Config) { c.Storage.DataDir = "" },
			wantMsg: "storage.data_dir is required",
		},
		{
			name:    "invalid sync_policy",
			mutate:  func(c *Config) { c.Storage.SyncPolicy = "turbo" },
			wantMsg: `storage.sync_policy "turbo" is invalid`,
		},
		{
			name: "zero sync_interval_ms with periodic policy",
			mutate: func(c *Config) {
				c.Storage.SyncPolicy = SyncPolicyPeriodic
				c.Storage.SyncIntervalMS = 0
			},
			wantMsg: "storage.sync_interval_ms must be positive",
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
			name:    "invalid tls min_version",
			mutate:  func(c *Config) { c.TLS.MinVersion = "1.1" },
			wantMsg: `tls.min_version "1.1" is invalid`,
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

// TestValidate_TLSMinVersion_Valid confirms that both accepted values pass.
func TestValidate_TLSMinVersion_Valid(t *testing.T) {
	for _, v := range []string{"1.2", "1.3"} {
		t.Run(v, func(t *testing.T) {
			c := minValidConfig()
			c.TLS.MinVersion = v
			assert.NoError(t, c.Validate())
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

		assert.Equal(t, SyncPolicyPeriodic, c.Storage.SyncPolicy)
		assert.Equal(t, 200, c.Storage.SyncIntervalMS)
		assert.Equal(t, 512, c.Storage.MaxSubscriberLagMB)
		assert.Equal(t, 256, c.Storage.CompactionThresholdMB)
		require.NotNil(t, c.Subscribers.MaxRetries)
		assert.Equal(t, 5, *c.Subscribers.MaxRetries)
		assert.Equal(t, "1.3", c.TLS.MinVersion)
		assert.Equal(t, 30, c.TLS.ExpiryWarnDays)
		assert.Equal(t, 500, c.Federation.ReconnectBaseMS)
		assert.Equal(t, 60_000, c.Federation.ReconnectMaxMS)
		assert.Equal(t, 0.2, c.Federation.ReconnectJitter)
		assert.Equal(t, 10_000, c.Federation.SendBufferMessages)
		assert.Equal(t, 65_536, c.Federation.MaxBatchBytes)
		assert.Equal(t, 100_000, c.Dedup.SeenIDLRUSize)
		assert.Equal(t, 100, c.Audit.MaxSizeMB)
		assert.Equal(t, 10, c.Audit.MaxFiles)
	})

	t.Run("explicit values are not overwritten", func(t *testing.T) {
		c := Config{
			Storage: StorageConfig{
				SyncPolicy:            SyncPolicyAlways,
				SyncIntervalMS:        999,
				MaxSubscriberLagMB:    1024,
				CompactionThresholdMB: 512,
			},
			Subscribers: SubscribersConfig{MaxRetries: intPtr(10)},
			TLS:         TLSConfig{MinVersion: "1.2", ExpiryWarnDays: 60},
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

		assert.Equal(t, SyncPolicyAlways, c.Storage.SyncPolicy)
		assert.Equal(t, 999, c.Storage.SyncIntervalMS)
		assert.Equal(t, 1024, c.Storage.MaxSubscriberLagMB)
		assert.Equal(t, 512, c.Storage.CompactionThresholdMB)
		require.NotNil(t, c.Subscribers.MaxRetries)
		assert.Equal(t, 10, *c.Subscribers.MaxRetries)
		assert.Equal(t, "1.2", c.TLS.MinVersion)
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

	t.Run("name defaults to hostname", func(t *testing.T) {
		var c Config
		c.ApplyDefaults()
		hostname, err := os.Hostname()
		require.NoError(t, err)
		assert.Equal(t, hostname, c.Name)
	})

	t.Run("explicit name not overwritten", func(t *testing.T) {
		c := Config{Name: "my-custom-host"}
		c.ApplyDefaults()
		assert.Equal(t, "my-custom-host", c.Name)
	})
}

// TestLoadConfig_RoundTrip writes a fully-populated config YAML to a temp file,
// loads it via LoadConfig, and asserts every field matches the original.
func TestLoadConfig_RoundTrip(t *testing.T) {
	const yaml = `
name: billing-host
storage:
  data_dir: /var/keyop
  sync_policy: always
  sync_interval_ms: 500
  max_subscriber_lag_mb: 1024
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
  min_version: "1.3"
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

	assert.Equal(t, "billing-host", cfg.Name)
	assert.Equal(t, "/var/keyop", cfg.Storage.DataDir)
	assert.Equal(t, SyncPolicyAlways, cfg.Storage.SyncPolicy)
	assert.Equal(t, 500, cfg.Storage.SyncIntervalMS)
	assert.Equal(t, 1024, cfg.Storage.MaxSubscriberLagMB)
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
	assert.Equal(t, "1.3", cfg.TLS.MinVersion)
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
storage:
  sync_policy: periodic
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0600))

	_, err := LoadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage.data_dir is required")
}
