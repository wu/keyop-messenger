// Package main provides example host2 that subscribes to messages.
package main

import (
	"context"
	"log/slog"
	"path"

	messenger "github.com/wu/keyop-messenger"
)

func host2(ctx context.Context, logger *slog.Logger, baseDir string) {

	tmpDir := path.Join(baseDir, "host2")
	logger.Info("host2: starting", "dataDir", tmpDir)

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
					Subscribe: []string{
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
	if err := m.RegisterPayloadType("com.example.Alert", Alert{}); err != nil {
		logger.Error("failed to register payload type", "error", err)
		panic(err)
	}

	// Subscribe before publishing so the handler sees the message.
	logger.Info("host2: subscribing to alerts topic on worker-1")
	if err := m.Subscribe(ctx, "alerts", "worker-1", func(_ context.Context, msg messenger.Message) error {
		a, ok := msg.Payload.(Alert)
		if !ok {
			logger.Error("failed to cast payload to Alert")
			return nil
		}
		logger.Info("host2: received",
			"message", a.Message,
			"origin", msg.Origin,
			"service", msg.ServiceName,
		)
		return nil
	}); err != nil {
		logger.Error("host2: failed to subscribe", "error", err)
		panic(err)
	}
	logger.Info("host2: subscribed successfully")

	logger.Info("host2: listening for messages from host1")
	<-ctx.Done()
	logger.Info("host2: shutting down")
}
