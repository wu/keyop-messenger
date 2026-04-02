package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Logger is the structured logging interface used internally.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// FileWatcher is satisfied by storage.ChannelWatcher; defined locally to keep
// tlsutil free of a direct storage import.
type FileWatcher interface {
	Watch(path string) (<-chan struct{}, error)
	Close() error
}

// BuildTLSConfig loads a certificate key-pair and CA pool from disk and returns
// a *tls.Config suitable for mTLS federation connections.
// MinVersion is TLS 1.3; ClientAuth is RequireAndVerifyClientCert.
func BuildTLSConfig(certFile, keyFile, caFile string, _ Logger) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("tlsutil: load key pair: %w", err)
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tlsutil: read CA file: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("tlsutil: no valid certificates in CA file %s", caFile)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		RootCAs:      caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ExtractCN returns the Common Name from a certificate's subject.
func ExtractCN(cert *x509.Certificate) string {
	return cert.Subject.CommonName
}

// CheckExpiry logs a warning via logger if cert expires within warnDays.
func CheckExpiry(cert *x509.Certificate, warnDays int, logger Logger) {
	deadline := time.Now().UTC().Add(time.Duration(warnDays) * 24 * time.Hour)
	if cert.NotAfter.Before(deadline) {
		logger.Warn("tlsutil: certificate expiring soon",
			"cn", cert.Subject.CommonName,
			"expires", cert.NotAfter.Format(time.RFC3339),
			"warn_days", warnDays,
		)
	}
}

// HotReloadTLS holds an atomically-swappable *tls.Config. Call Config() to
// obtain a *tls.Config whose GetConfigForClient callback always reads the
// current (most recently reloaded) configuration.
type HotReloadTLS struct {
	current  atomic.Pointer[tls.Config]
	outerCfg *tls.Config
	stop     chan struct{}
	wg       sync.WaitGroup
}

// HotReloadTLSConfig builds an initial TLS config from disk, then starts a
// goroutine that rebuilds and atomically replaces it whenever certFile, keyFile,
// or caFile changes according to watcher.
func HotReloadTLSConfig(certFile, keyFile, caFile string, watcher FileWatcher, logger Logger) (*HotReloadTLS, error) {
	initial, err := BuildTLSConfig(certFile, keyFile, caFile, logger)
	if err != nil {
		return nil, err
	}

	h := &HotReloadTLS{stop: make(chan struct{})}
	h.current.Store(initial)

	// outerCfg is the stable handle callers hold. Its only job is to delegate
	// to the atomic pointer so new connections always use the latest config.
	h.outerCfg = &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			return h.current.Load(), nil
		},
	}

	// Watch all three files; any change triggers a full reload.
	merged, err := h.mergeWatches(watcher, certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			select {
			case <-merged:
				cfg, err := BuildTLSConfig(certFile, keyFile, caFile, logger)
				if err != nil {
					logger.Error("tlsutil: reload TLS config failed", "err", err)
					continue
				}
				h.current.Store(cfg)
				logger.Info("tlsutil: TLS config reloaded", "cert", certFile)
			case <-h.stop:
				return
			}
		}
	}()

	return h, nil
}

// Config returns the stable *tls.Config whose GetConfigForClient always
// delegates to the most recently loaded configuration.
func (h *HotReloadTLS) Config() *tls.Config {
	return h.outerCfg
}

// Close stops the reload goroutine and waits for it to exit.
func (h *HotReloadTLS) Close() {
	close(h.stop)
	h.wg.Wait()
}

// mergeWatches registers each path with watcher and fans notifications from
// all three into a single coalescing channel.
func (h *HotReloadTLS) mergeWatches(watcher FileWatcher, paths ...string) (<-chan struct{}, error) {
	merged := make(chan struct{}, 1)
	for _, path := range paths {
		ch, err := watcher.Watch(path)
		if err != nil {
			return nil, fmt.Errorf("tlsutil: watch %s: %w", path, err)
		}
		h.wg.Add(1)
		go func(src <-chan struct{}) {
			defer h.wg.Done()
			for {
				select {
				case _, ok := <-src:
					if !ok {
						return
					}
					select {
					case merged <- struct{}{}:
					default:
					}
				case <-h.stop:
					return
				}
			}
		}(ch)
	}
	return merged, nil
}
