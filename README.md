# Keyop Messenger

[![Go Reference](https://pkg.go.dev/badge/github.com/wu/keyop-messenger.svg)](https://pkg.go.dev/github.com/wu/keyop-messenger)

## What is Keyop Messenger?

[keyop-messenger](https://github.com/wu/keyop-messenger) is a lightweight, federated pub-sub system with at-least-once delivery guarantees, offline resilience, and mTLS security.  It was designed to run efficiently on resource-constrained systems with minimal operational complexity.

I created it to be the nervous system for 'keyop', the latest generation of an IoT project I've been working on since 1999.

For more details, see the [Getting Started](https://blog.geekfarm.org/introducing-keyop-messenger.html) document.

## Key Features

- **Reliable at-least-once delivery** — Messages published locally are durably persisted and guaranteed for delivery.
- **Federated** — Clients publish and subscribe to specific channels on hubs with configurable access control.
- **Offline capable** — Local event processing continues during disconnections, with automatic reconnection and message delivery upon restoration.
- **Secure** — mTLS authentication with per-instance certificates and automated certificate generation.
- **Low-latency** — Sub-second end-to-end latency across multiple federation hops.
- **Resource efficient** — Minimal memory and CPU footprint suitable for resource-constrained systems.
- **Simple** — Designed for ease of operation and maintenance with minimal complexity.
- **Observable** — Comprehensive audit trails and correlation IDs for troubleshooting multi-event processes.
- **Pure Go** — No CGO dependencies for simple cross-platform deployment.

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
    m.Publish(pubCtx, "alerts", "com.example.Alert", Alert{Message: "system problem!"})
}
```
### Correlation IDs

Correlation IDs track related messages across multi-step processes, allowing you to trace a single event through multiple services. Extract a message's ID and use it as the correlation ID for all downstream actions.

```go
// Temperature sensor publishes a reading
m.Publish(context.Background(), "temp", "com.example.TempReading", &reading)

// Thermostat subscribes to temperature readings and reacts
m.Subscribe(context.Background(), "temp", "thermostat", func(ctx context.Context, msg messenger.Message) error {
    // Use the temp message's ID as the correlation ID for downstream actions
    downstreamCtx := messenger.WithCorrelationID(context.Background(), msg.ID)
    
    // Publish alert and switch events with the correlation ID
    m.Publish(downstreamCtx, "alerts", "com.example.Alert", &alert)
    m.Publish(downstreamCtx, "heater", "com.example.SwitchEvent", &switchEvent)
    
    return nil
})

// Switch service receives events and can trace them back to the original sensor reading
m.Subscribe(context.Background(), "heater", "switch", func(ctx context.Context, msg messenger.Message) error {
    slog.Info("heater activated",
        "correlationId", msg.CorrelationID,  // Traces back to the original temp reading
        "origin", msg.Origin,
        "id", msg.ID,
    )
    return nil
})
```

### Service Names

Service names identify which service published a message. Set a service name via context before publishing, and it will be stamped on the envelope and delivered to subscribers. Useful for debugging and log triage.

```go
// Publish from a specific service
ctx := messenger.WithServiceName(context.Background(), "temperature-sensor")
m.Publish(ctx, "sensors", "com.example.TemperatureReading", &reading)

// Subscribers can see which service published the message
m.Subscribe(ctx, "sensors", "monitor", func(ctx context.Context, msg messenger.Message) error {
    slog.Info("temperature recorded",
        "service", msg.ServiceName,  // "temperature-sensor"
        "origin", msg.Origin,        // instance name
        "id", msg.ID,
    )
    return nil
})
```

Service names work well with correlation IDs — both can be set in the same context:

```go
ctx := messenger.WithServiceName(context.Background(), "thermostat")
ctx = messenger.WithCorrelationID(ctx, sourceMsg.CorrelationID)

m.Publish(ctx, "heater", "com.example.SwitchEvent", &switchEvent)
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

## Development Commands

```bash
make test               # unit tests with race detector
make test-integration   # integration tests (build tag: integration)
make bench              # benchmarks
make lint               # golangci-lint
make build              # verify compilation
```

## License

BSD 2-Clause License. See [LICENSE](./LICENSE) for details.
