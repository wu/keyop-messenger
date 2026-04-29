// Package main provides example host1 that publishes messages.
package main

import (
	"context"
	"log/slog"
	"path"

	messenger "github.com/wu/keyop-messenger"
)

// host1 is acting as the 'hub'
func host1(ctx context.Context, logger *slog.Logger, baseDir string) {

	tmpDir := path.Join(baseDir, "host1")
	logger.Info("host1: starting", "dataDir", tmpDir)

	// listen on port (7740) and waits for clients to connect
	cfg := &messenger.Config{
		Name: "host1",
		Storage: messenger.StorageConfig{
			DataDir: tmpDir,
		},
		Hub: messenger.HubConfig{
			Enabled:    true,
			ListenAddr: ":7740",
			// no channels are specified for 'localhost', so it can publish and subscribe to any channels it specifies
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

	// Create a new instance of the messenger for this host.
	// Normally this would only happen once per host, but this example is showing federation on a single host.
	m, err := messenger.New(cfg, messenger.WithLogger(logger))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			logger.Error("failed to close messenger", "error", err)
		}
	}()

	// Register each of your payload types for typed decoding.
	if err := m.RegisterPayloadType("com.example.alert.v1", Alert{}); err != nil {
		panic(err)
	}

	// Subscribe to the local 'alerts' channel, and run the callback on each message that arrives there.
	// The subscriber ID is used to record your local offset in the channel log.
	logger.Info("host1: subscribing to alerts topic on worker-1")
	err = m.Subscribe(ctx, "alerts", "worker-1", func(_ context.Context, msg messenger.Message) error {

		// check the payload type/version and respond accordingly
		if msg.PayloadType == "com.example.alert.v1" {
			// decode the payload into the expected struct type
			a, ok := msg.Payload.(Alert)
			if !ok {
				logger.Error("failed to cast payload to Alert")
				return nil
			}

			// Just log the event details
			logger.Info("host1: received",
				"details", a.Details,
				"origin", msg.Origin,
				"service", msg.ServiceName,
				"timestamp", msg.Timestamp.Format("2006-01-02 15:04:05"),
				"count", a.Count,
			)
		}

		// Return nil to Ack the message, and allow the offset to be advanced.
		// Returning an error will cause the message to be retried after a backoff period.
		return nil
	})
	if err != nil {
		logger.Error("host1: failed to subscribe", "error", err)
		panic(err)
	}
	logger.Info("host1: subscribed successfully")

	logger.Info("host1: listening for messages from host1")
	<-ctx.Done()
	logger.Info("host1: shutting down")
}
