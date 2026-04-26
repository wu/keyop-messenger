package main

import (
	"context"
	"log/slog"
	"path"
	"time"

	messenger "github.com/wu/keyop-messenger"
)

func host1(ctx context.Context, logger *slog.Logger, baseDir string) {

	tmpDir := path.Join(baseDir, "host1")
	logger.Info("host1: starting", "dataDir", tmpDir)

	cfg := &messenger.Config{
		Name: "host1",
		Storage: messenger.StorageConfig{
			DataDir: tmpDir,
		},
		Hub: messenger.HubConfig{
			Enabled:    true,
			ListenAddr: ":7740",
			AllowedPeers: []messenger.AllowedPeer{
				{
					Name: "localhost",
				},
			},
		},
		TLS: messenger.TLSConfig{
			Cert: path.Join(tmpDir, "cert", "host1.crt"),
			Key:  path.Join(tmpDir, "cert", "host1.key"),
			CA:   path.Join(tmpDir, "cert", "ca.crt"),
		},
	}
	cfg.ApplyDefaults()

	m, err := messenger.New(cfg, messenger.WithLogger(logger))
	if err != nil {
		panic(err)
	}
	defer m.Close()

	// Register payload types for typed decoding.
	if err := m.RegisterPayloadType("com.example.Alert", Alert{}); err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("host1: shutting down")
			return
		default:
			// Publish with service identification. Blocks until write is confirmed to disk.
			pubCtx := messenger.WithServiceName(ctx, "monitor-service")
			logger.Info("host1: publishing message", "service", "monitor-service")
			if err := m.Publish(pubCtx, "alerts", "com.example.Alert", Alert{Message: "system problem!"}); err != nil {
				slog.Error("failed to publish message", "error", err)
			}
			time.Sleep(time.Second)
		}
	}
}
