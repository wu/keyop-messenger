# Keyop Messenger

Keyop Messenger is a high-reliability, file-based pub-sub library for Go. It is designed for systems where durability and delivery guarantees are paramount, offering a simple yet robust architecture based on append-only `.jsonl` files, persistent offset tracking, and mTLS-secured federation.

## Key Features

- **At-Least-Once Delivery**: Messages are only committed (offset advanced) after successful handler execution.
- **Durable Storage**: Every channel is a directory of fixed-size segment files. Atomic appends ensure no record interleaving. Old segments are deleted once all subscribers have consumed them.
- **Persistent Offset Tracking**: Subscribers resume exactly where they left off, even after a crash or restart.
- **Low-Latency Dispatch**: Uses a dual-layer notification system (in-process `LocalNotifier` + `fsnotify` for filesystem events).
- **Type-Safe Payloads**: Built-in registry for decoding message bodies into structured Go types.
- **Reliable Retries & DLQ**: Configurable retry logic with automatic routing to `.dead-letter` channels.
- **Secure Federation**: Star-topology federation over mTLS WebSockets. Clients subscribe to specific channels; hubs enforce per-client channel allowlists under explicit, hot-reloadable policy.
- **Observability**: File-based offsets and JSONL records allow operators to use standard Unix tools (`cat`, `grep`, `tail`) for debugging.

## Why Keyop Messenger?

Unlike memory-based message brokers, Keyop Messenger treats the filesystem as the single source of truth. This makes it:
1. **Resilient**: No complex cluster state to manage. If the file is there, the data is safe.
2. **Transparent**: Debugging a stuck subscriber is as simple as `cat subscriber.offset`.
3. **Low-Overhead**: No separate broker process is required for local-only messaging.

## Quick Start

### Installation

```bash
go get github.com/wu/keyop-messenger
```

### Basic Usage

```go
package main

import (
    "context"
    "log/slog"

    messenger "github.com/wu/keyop-messenger"
)

type Alert struct {
    Message string `json:"message"`
}

func main() {
    cfg := &messenger.Config{
        Name: "my-instance",
        Storage: messenger.StorageConfig{
            DataDir: "/var/keyop/my-instance",
        },
    }
    cfg.ApplyDefaults()

    m, err := messenger.New(cfg, messenger.WithLogger(slog.Default()))
    if err != nil {
        panic(err)
    }
    defer m.Close()

    // Register payload types for typed decoding.
    m.RegisterPayloadType("com.example.Alert", Alert{})

    ctx := context.Background()

    // Subscribe before publishing so the handler sees the message.
    m.Subscribe(ctx, "alerts", "worker-1", func(ctx context.Context, msg messenger.Message) error {
        a := msg.Payload.(Alert)
        slog.Info("received", "message", a.Message, "origin", msg.Origin)
        return nil
    })

    // Publish blocks until the write is confirmed to disk.
    m.Publish(ctx, "alerts", "com.example.Alert", Alert{Message: "system heat!"})
}
```

### Certificate Generation (for Federation)

```bash
# Install the CLI
go install github.com/wu/keyop-messenger/cmd/keyop-messenger@latest

# Generate a CA (once per cluster)
keyop-messenger keygen ca --out-cert ca.crt --out-key ca.key

# Generate a per-instance certificate
keyop-messenger keygen instance \
  --ca ca.crt --ca-key ca.key \
  --name billing-host \
  --out-cert billing-host.crt \
  --out-key  billing-host.key
```

## Architecture

Keyop Messenger follows a **Hub-and-Spoke** model:
- **Clients**: Connect to a local Hub to publish or subscribe to channels.
- **Hubs**: Manage local `.jsonl` files and coordinate with peer Hubs.
- **Channels**: Each channel is a directory of append-only `.jsonl` segment files. Once all subscribers consume a segment it is deleted — no copying, no writer pauses.
- **Offsets**: Each subscriber has a unique `.offset` file tracking its last read byte position across all segments.

## Development Commands

```bash
make test               # unit tests with race detector
make test-integration   # integration tests (build tag: integration)
make bench              # benchmarks
make lint               # golangci-lint
make build              # verify compilation
```

Or directly:

```bash
go test -race ./...
go test -race -tags integration -timeout 60s ./...
go test -run='^$' -bench=. -benchmem -benchtime=3s ./...
golangci-lint run ./...
```

## Project Status

All 15 phases are complete. The library is feature-complete and integration-tested.

- [x] Phase 1: Module Scaffold & Configuration
- [x] Phase 2: Message Envelope & Payload Registry
- [x] Phase 3: Durable Writer Goroutine (Atomic Append, Segment Rolling)
- [x] Phase 4: Offset Tracking & File Watcher
- [x] Phase 5: Subscriber Engine & Dead-Letter Queues
- [x] Phase 6: Storage Compaction (Segment Deletion)
- [x] Phase 7: Deduplication (LRU Seen-ID Set)
- [x] Phase 8: Audit Log
- [x] Phase 9: TLS Utilities & Certificate Generation
- [x] Phase 10: Federation Wire Framing & Handshake
- [x] Phase 11: Federation Policy Engine & Hot-Reload
- [x] Phase 12: Federation Hub, Client & Peer Goroutines
- [x] Phase 13: Root Messenger API
- [x] Phase 14: CLI (`keygen` subcommands)
- [x] Phase 15: Hardening, Benchmarks & Integration Tests

See [DESIGN.md](./DESIGN.md) for architecture details and [IMPLEMENTATION.md](./IMPLEMENTATION.md) for the full roadmap.

## License

BSD 2-Clause License. See [LICENSE](./LICENSE) for details.
