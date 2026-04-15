# Keyop Messenger

[![Go Reference](https://pkg.go.dev/badge/github.com/wu/keyop-messenger.svg)](https://pkg.go.dev/github.com/wu/keyop-messenger)

Keyop Messenger is a high-reliability, file-based pub-sub library for Go. It is designed for systems where durability and delivery guarantees are paramount, offering a simple yet robust architecture based on append-only `.jsonl` files, persistent offset tracking, and mTLS-secured federation.

NOTE: This is still Beta, the API should now be relatively stable.

See also:  [DESIGN.md](./DESIGN.md) for detailed design rationale and architecture.

## Key Features

- **At-Least-Once Delivery**: Messages are only committed (offset advanced) after successful handler execution.
- **Durable Storage**: Every channel is a directory of fixed-size segment files. Atomic appends ensure no record interleaving. Old segments are deleted once all subscribers have consumed them.
- **Persistent Offset Tracking**: Subscribers resume exactly where they left off, even after a crash or restart.
- **Low-Latency Dispatch**: Uses a dual-layer notification system (in-process `LocalNotifier` + `fsnotify` for filesystem events).
- **Type-Safe Payloads**: Built-in registry for decoding message bodies into structured Go types.
- **Correlation IDs**: Stamp messages with application-level correlation IDs to trace multi-step processes across service boundaries.
- **Reliable Retries & DLQ**: Configurable retry logic with automatic routing to `.dead-letter` channels.
- **Secure Federation**: Star-topology federation over mTLS WebSockets. Clients subscribe to specific channels; hubs enforce per-client channel allowlists under explicit policy.
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
        slog.Info("received",
            "message", a.Message,
            "origin", msg.Origin,
            "service", msg.ServiceName,
        )
        return nil
    })

    // Publish with service identification. Blocks until the write is confirmed to disk.
    pubCtx := messenger.WithServiceName(ctx, "monitor-service")
    m.Publish(pubCtx, "alerts", "com.example.Alert", Alert{Message: "system heat!"})
}
```

### Correlation IDs

Correlation IDs track related messages across multi-step processes. Set a correlation ID via context before publishing, and it will be stamped on the envelope and delivered to subscribers. Useful for tracing a request through multiple services.

```go
// Start a correlated chain of messages
ctx := messenger.WithCorrelationID(context.Background(), "order-123")

// All messages published with this context carry the same correlation ID
m.Publish(ctx, "orders", "com.example.OrderCreated", &order)
m.Publish(ctx, "payments", "com.example.ChargeOrder", &charge)
m.Publish(ctx, "shipping", "com.example.ShipOrder", &shipment)

// Subscribers receive the correlation ID
m.Subscribe(ctx, "orders", "processor", func(ctx context.Context, msg messenger.Message) error {
    // msg.CorrelationID == "order-123"

    // Propagate to downstream services
    downstreamCtx := messenger.WithCorrelationID(ctx, msg.CorrelationID)
    m.Publish(downstreamCtx, "next-channel", "com.example.NextEvent", &event)

    return nil
})
```

### Service Names

Service names identify which service published a message. Set a service name via context before publishing, and it will be stamped on the envelope and delivered to subscribers. Useful for debugging and log triage.

```go
// Publish from a specific service
ctx := messenger.WithServiceName(context.Background(), "payment-processor")
m.Publish(ctx, "payments", "com.example.ChargeCompleted", &charge)

// Subscribers can see which service published the message
m.Subscribe(ctx, "payments", "auditor", func(ctx context.Context, msg messenger.Message) error {
    slog.Info("payment processed",
        "service", msg.ServiceName,  // "payment-processor"
        "origin", msg.Origin,        // instance name
        "id", msg.ID,
    )
    return nil
})
```

Service names work well with correlation IDs — both can be set in the same context:

```go
ctx := messenger.WithServiceName(context.Background(), "orders")
ctx = messenger.WithCorrelationID(ctx, "order-789")

m.Publish(ctx, "orders", "com.example.OrderCreated", &order)
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

## Ephemeral Client

`EphemeralMessenger` connects to a hub without maintaining any local storage. Use it when:

- You need to **publish with delivery confirmation** (blocks until the hub has written the message to disk) but do not want to manage a data directory.
- You want to **receive live messages** only while connected, with no replay of messages published during a disconnect.

### Publishing

```go
em, err := messenger.NewEphemeralMessenger(messenger.EphemeralConfig{
    HubAddr:      "hub.example.com:7740",
    InstanceName: "transient-service",
    TLS: messenger.TLSConfig{
        Cert: "transient-service.crt",
        Key:  "transient-service.key",
        CA:   "ca.crt",
    },
})
if err != nil {
    panic(err)
}
defer em.Close()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := em.Connect(ctx); err != nil {
    panic(err)
}

// Publish blocks until the hub acks (message is on disk) or ctx expires.
err = em.Publish(ctx, "channelname", "com.example.Event", order)
if errors.Is(err, messenger.ErrEphemeralConnLost) {
    // Connection dropped before ack — message may or may not have been received.
    // Retry if idempotent; check or alert otherwise.
}
```

### Subscribing

```go
em, err := messenger.NewEphemeralMessenger(messenger.EphemeralConfig{
    HubAddr:      "hub.example.com:7740",
    InstanceName: "dashboard",
    Subscribe:    []string{"metrics"},   // declare channels before Connect
    TLS:          messenger.TLSConfig{Cert: "dashboard.crt", Key: "dashboard.key", CA: "ca.crt"},
    AutoReconnect: true,
})

em.Subscribe("metrics", func(msg messenger.Message) {
    fmt.Println("live metric:", msg.Payload)
})

em.Connect(ctx) // returns after first connection; reconnects in background
```

Handler errors are logged but do not stop delivery. On reconnect, delivery resumes from the current hub position — messages published while disconnected are never replayed.

### Auto-Reconnect

Set `AutoReconnect: true` to reconnect automatically with exponential backoff after a disconnect. The default backoff starts at 500 ms and caps at 60 s with ±20% jitter. Pending `Publish` calls that have not yet been enqueued block until reconnected; in-flight calls at the moment of disconnect return `ErrEphemeralConnLost`.

### Differences from `Messenger`

|                  | `Messenger`                         | `EphemeralMessenger`                                            |
|------------------|-------------------------------------|-----------------------------------------------------------------|
| Local storage    | `.jsonl` files per channel          | None                                                            |
| Subscribe replay | Resumes from last offset on restart | No replay; live-only                                            |
| Publish ack      | Write confirmed to local disk       | Hub confirmed to disk; connection loss = `ErrEphemeralConnLost` |
| Data directory   | Required                            | Not required                                                    |

---

## Architecture

Keyop Messenger follows a **Hub-and-Spoke** model:
- **Clients**: Connect to a Hub to publish or subscribe to channels.
- **Hubs**: Manage local `.jsonl` files, coordinate with peer Hubs, and deliver messages to subscribers using a **file-reader pull model**.
- **Channels**: Each channel is a directory of append-only `.jsonl` segment files. Once all subscribers (local and federation) have consumed a segment it is deleted — no copying, no writer pauses.
- **Offsets**: Each subscriber has a unique `.offset` file tracking its last read byte position across all segments. Federation peer offsets are stored under `subscribers/{channel}/fed-{peerName}.offset` and are automatically included in compaction boundary calculations.

### Federation Delivery Model

The hub delivers messages to subscribed federation peers using the same mechanism as local subscribers:

1. When a message is written to a channel, the hub calls `NotifyChannel(channel)`, waking a `channelReader` goroutine for each peer subscribed to that channel.
2. The `channelReader` reads from segment files starting at the peer's last byte offset, batches envelopes (up to `max_batch_bytes`), and delivers them via WebSocket.
3. After the peer acknowledges the batch, the byte offset is persisted atomically to `subscribers/{channel}/fed-{peerName}.offset`.
4. On reconnect, delivery resumes from the stored offset — no `last_id` handshake field is needed.

Offset files for peers that disconnect and never reconnect are cleaned up by a TTL sweep (configurable via `hub.fed_client_offset_ttl`, default 1 week).

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

## License

BSD 2-Clause License. See [LICENSE](./LICENSE) for details.
