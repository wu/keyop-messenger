// Package main provides example host2 that subscribes to messages.
package main

import (
	"context"
	"log/slog"
	"path"
	"time"

	messenger "github.com/wu/keyop-messenger"
)

// host2 is acting as the client
func host2(ctx context.Context, logger *slog.Logger, baseDir string) {

	tmpDir := path.Join(baseDir, "host2")
	logger.Info("host2: starting", "dataDir", tmpDir)

	// connect to the hub on 'localhost' and subscribe to the 'alerts' channel.
	cfg := &messenger.Config{
		Name: "host2",
		Storage: messenger.StorageConfig{
			DataDir: tmpDir,
		},
		Client: messenger.ClientConfig{
			Enabled: true,
			Hubs: []messenger.ClientHubRef{
				{
					Addr: "localhost:7740",
					Publish: []string{
						"alerts",
					},
				},
			},
		},
		TLS: messenger.TLSConfig{
			Cert: path.Join(tmpDir, "cert", "host2.crt"),
			Key:  path.Join(tmpDir, "cert", "host2.key"),
			CA:   path.Join(tmpDir, "cert", "ca.crt"),
		},
	}
	cfg.ApplyDefaults()

	// Create a new instance of the messenger for this host.
	// Normally this would only happen once per host, but this example is showing federation on a single host.
	logger.Info("host2: creating messenger instance", "config", cfg)
	m, err := messenger.New(cfg, messenger.WithLogger(logger))
	if err != nil {
		logger.Error("failed to create messenger", "error", err)
		panic(err)
	}
	logger.Info("host2: messenger created successfully")
	defer func() {
		if err := m.Close(); err != nil {
			logger.Error("failed to close messenger", "error", err)
		}
	}()

	// Register payload types for typed decoding.
	if err := m.RegisterPayloadType("com.example.alert.v1", Alert{}); err != nil {
		logger.Error("failed to register payload type", "error", err)
		panic(err)
	}

	count := 0
	for {
		select {
		case <-ctx.Done():
			logger.Info("host2: shutting down")
			return
		default:
			// Publish to local channel with service identification.
			pubCtx := messenger.WithServiceName(ctx, "monitor-service")
			logger.Info("host2: publishing message", "service", "monitor-service")

			count++
			alert := Alert{Message: "system problem!", Count: count}
			// send the data using the registered payload type.   Blocks until write is confirmed to disk.
			if err := m.Publish(pubCtx, "alerts", "com.example.alert.v1", alert); err != nil {
				slog.Error("failed to publish message", "error", err)
			}
			time.Sleep(time.Second)
		}
	}
}
