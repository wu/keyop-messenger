# Keyop Messenger

Keyop Messenger is a high-reliability, file-based pub-sub library for Go. It is designed for systems where durability and delivery guarantees are paramount, offering a simple yet robust architecture based on append-only `.jsonl` files, persistent offset tracking, and mTLS-secured federation.

## Key Features

- **At-Least-Once Delivery**: Messages are only committed (offset advanced) after successful handler execution.
- **Durable Storage**: Every channel is a directory of fixed-size segment files. Atomic appends ensure no record interleaving. Old segments are deleted once all subscribers have consumed them.
- **Persistent Offset Tracking**: Subscribers resume exactly where they left off, even after a crash or restart.
- **Low-Latency Dispatch**: Uses a dual-layer notification system (in-process `LocalNotifier` + `fsnotify` for filesystem events).
- **Type-Safe Payloads**: Built-in registry for decoding message bodies into structured Go types.
- **Reliable Retries & DLQ**: Configurable retry logic with automatic routing to `.dead-letter` channels.
- **Secure Federation**: Designed for star-topology federation using mTLS over WebSockets (Phase 12+).
- **Observability**: File-based offsets and JSONL records allow operators to use standard Unix tools (`cat`, `grep`, `tail`) for debugging.

## Why Keyop Messenger?

Unlike memory-based message brokers, Keyop Messenger treats the filesystem as the single source of truth. This makes it:
1. **Resilient**: No complex cluster state to manage. If the file is there, the data is safe.
2. **Transparent**: Debugging a stuck subscriber is as simple as `cat subscriber.offset`.
3. **Low-Overhead**: No separate broker process is required for local-only messaging.

## Quick Start

### Installation

```bash
go get github.com/keyop/keyop-messenger
```

### Basic Usage (Coming Soon)

The library is currently in active development. Below is the intended API for Phase 13+:

```go
package main

import (
    "context"
    "github.com/keyop/keyop-messenger"
)

type MyPayload struct {
    Message string `json:"message"`
}

func main() {
    ctx := context.Background()
    
    // 1. Initialize with config
    cfg := messenger.DefaultConfig()
    m, _ := messenger.New(cfg)
    defer m.Close()

    // 2. Register your types
    m.RegisterPayloadType("com.example.Alert", MyPayload{})

    // 3. Subscribe to a channel
    m.Subscribe(ctx, "alerts", "worker-1", func(ctx context.Context, msg messenger.Message) error {
        payload := msg.Payload.(MyPayload)
        println("Received:", payload.Message)
        return nil
    })

    // 4. Publish a message
    m.Publish(ctx, "alerts", "com.example.Alert", MyPayload{Message: "System Heat!"})
}
```

## Architecture

Keyop Messenger follows a **Hub-and-Spoke** model:
- **Clients**: Connect to a local Hub to publish or subscribe to channels.
- **Hubs**: Manage local `.jsonl` files and coordinate with peer Hubs.
- **Channels**: Each channel is a directory of append-only `.jsonl` segment files. Once all subscribers consume a segment it is deleted — no copying, no writer pauses.
- **Offsets**: Each subscriber has a unique `.offset` file tracking its last read byte position across all segments.

## Project Status

Keyop Messenger is currently in **Phase 12** of its development plan:
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
- [ ] Phase 12: Federation Hub, Client & Peer Goroutines (Next)
- [ ] Phase 13: Root Messenger API
- [ ] Phase 14: CLI (`keygen` subcommands)
- [ ] Phase 15: Hardening, Benchmarks & Integration Tests

See [DESIGN.md](./DESIGN.md) for architecture details and [IMPLEMENTATION.md](./IMPLEMENTATION.md) for the full roadmap.

## License

BSD 2-Clause License. See [LICENSE](./LICENSE) for details.
