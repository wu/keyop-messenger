package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Alert struct {
	Details string `json:"message"`
	Count   int
}

func main() {

	logger := slog.Default()

	baseDir := os.Getenv("KEYOP_MESSENGER_TMPDIR")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for interrupt signal (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		host1(ctx, logger, baseDir)
	}()

	go func() {
		defer wg.Done()
		host2(ctx, logger, baseDir)
	}()

	wg.Wait()

	logger.Warn("removing temp directory", "path", baseDir)
	// G703: baseDir is controlled via environment variable for testing purposes
	// #nosec G703
	if err := os.RemoveAll(baseDir); err != nil {
		slog.Error("failed to remove temp directory", "error", err)
	}
}
