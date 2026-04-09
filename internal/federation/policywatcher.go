//nolint:gosec // G304: reads policy file from trusted config path
package federation

import (
	"fmt"
	"os"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/wu/keyop-messenger/internal/audit"
	"gopkg.in/yaml.v3"
)

// logger is the structured logging interface used within the federation package.
type logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// HubApplier is implemented by Hub (Phase 12). Defined here so PolicyWatcher
// can be compiled and tested without importing the concrete Hub type.
type HubApplier interface {
	ApplyPolicy(cfg HubConfig)
}

// PolicyWatcher watches a YAML config file for changes and calls
// hub.ApplyPolicy on each valid reload.
type PolicyWatcher struct {
	path    string
	hub     HubApplier
	auditL  audit.AuditLogger
	log     logger
	watcher *fsnotify.Watcher
	stop    chan struct{}
	wg      sync.WaitGroup
}

// NewPolicyWatcher creates a PolicyWatcher and starts the background file
// watch goroutine. configPath must point to an existing YAML file that can
// be parsed as HubConfig.
func NewPolicyWatcher(configPath string, hub HubApplier, auditL audit.AuditLogger, log logger) (*PolicyWatcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("federation: create fsnotify watcher: %w", err)
	}
	if err := fw.Add(configPath); err != nil {
		_ = fw.Close()
		return nil, fmt.Errorf("federation: watch %s: %w", configPath, err)
	}

	pw := &PolicyWatcher{
		path:    configPath,
		hub:     hub,
		auditL:  auditL,
		log:     log,
		watcher: fw,
		stop:    make(chan struct{}),
	}

	pw.wg.Add(1)
	go pw.run()
	return pw, nil
}

// Close stops the watcher goroutine and releases resources.
func (pw *PolicyWatcher) Close() error {
	close(pw.stop)
	pw.wg.Wait()
	return pw.watcher.Close()
}

// run is the background goroutine that reacts to fsnotify events.
func (pw *PolicyWatcher) run() {
	defer pw.wg.Done()
	for {
		select {
		case event, ok := <-pw.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) {
				pw.reload()
			}
		case err, ok := <-pw.watcher.Errors:
			if !ok {
				return
			}
			pw.log.Error("federation: policywatcher fsnotify error", "err", err)
		case <-pw.stop:
			return
		}
	}
}

// reload parses the config file and, if valid, calls hub.ApplyPolicy.
func (pw *PolicyWatcher) reload() {
	cfg, err := loadHubConfig(pw.path)
	if err != nil {
		pw.log.Error("federation: policy reload failed", "path", pw.path, "err", err)
		_ = pw.auditL.Log(audit.Event{
			Event:  audit.EventPolicyReloadFailed,
			Detail: err.Error(),
		})
		return
	}

	pw.hub.ApplyPolicy(cfg)
	pw.log.Info("federation: policy reloaded", "path", pw.path)
	_ = pw.auditL.Log(audit.Event{
		Event:  audit.EventPolicyReloaded,
		Detail: pw.path,
	})
}

// loadHubConfig reads configPath, parses it as YAML, and validates it.
func loadHubConfig(path string) (HubConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return HubConfig{}, fmt.Errorf("read config: %w", err)
	}
	var cfg HubConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return HubConfig{}, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return HubConfig{}, err
	}
	return cfg, nil
}
