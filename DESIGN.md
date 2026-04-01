# Keyop Messenger — Design Document

## 1. Overview

Keyop Messenger is a pub-sub messaging library for distributed Go applications. It provides:

- **Reliable at-least-once delivery** backed by append-only `.jsonl` log files
- **Fan-out isolation** — slow subscribers do not affect other subscribers or publishers
- **Federated messaging** — instances connect via WebSocket with mutual TLS; hubs forward select channels to peer hubs under explicit policy
- **Policy-driven routing** — hub-to-hub channel forwarding is statically configured and hot-reloaded without restart
- **Dead letter queue** — messages that exceed the retry limit are moved to a dead-letter channel rather than blocking delivery
- **Audit logging** — all cross-hub message forwarding is recorded with automatic rotation

---

## 2. Concepts

| Term | Definition |
|---|---|
| **Instance** | A single running process embedding the messenger library |
| **Instance name** | Human-readable identifier for an instance; defaults to OS hostname. If multiple instances share a host, append the port: `hostname:port`. |
| **Channel** | A named, ordered stream of messages (analogous to a Kafka topic) |
| **Publisher** | Code that appends a message to a channel |
| **Subscriber** | Code that reads and processes messages from a channel |
| **Hub** | An instance that accepts inbound WebSocket connections from clients and/or peer hubs |
| **Client** | An instance that dials outbound to one or more hubs |
| **Peer hub** | A hub-to-hub WebSocket connection carrying forwarded messages |
| **Dead-letter channel** | A channel named `{channel}.dead-letter` that receives messages the subscriber has failed to process after the maximum retry count |

An instance may be a hub, a client, or both simultaneously.

---

## 3. Topology

### 3.1 Star with Multiple Hubs

Clients connect only to hubs. Hubs connect to peer hubs. Clients never connect directly to each other.

```
Private Network                        External Network
────────────────                       ────────────────
Instance A ─┐                          Instance D ─┐
Instance B ─┼──→ Hub 1 ──(selected)──→ Hub 2 ←────┤ Instance E
Instance C ─┘    channels only         └──(selected)──→ Instance F
                                           channels only
```

A single instance may simultaneously accept client connections (hub role) and dial outbound to peer hubs (client role). In this case it acts as a relay between its local clients and the wider federation.

### 3.2 What Is Not Supported

- Direct client-to-client connections
- Automatic channel discovery or subscription propagation between hubs
- Wildcard channel patterns in forwarding policy (exact channel names only)

---

## 4. Message Format

### 4.1 Envelope

Every message written to a `.jsonl` file is a single JSON object on one line:

```json
{
  "v":            1,
  "id":           "01952c3e-7b2a-7c4d-9f1a-3e8d2b1c0a5f",
  "ts":           "2026-03-31T14:22:01.123456789Z",
  "channel":      "orders",
  "origin":       "billing-host",
  "payload_type": "com.keyop.orders.OrderCreated",
  "payload":      { ... }
}
```

| Field | Type | Description |
|---|---|---|
| `v` | int | Envelope schema version. Currently `1`. |
| `id` | string | UUID v4 assigned by the publisher. Globally unique. Used for deduplication only — not the instance identifier. |
| `ts` | string | RFC3339Nano timestamp at time of publish, in UTC. |
| `channel` | string | The channel this message was published to. |
| `origin` | string | Instance name of the original publisher. Preserved across hub forwarding. |
| `payload_type` | string | Fully-qualified type discriminator for the payload. Reverse-DNS format recommended. |
| `payload` | object | Arbitrary JSON object. Shape is defined by the payload type. |

The envelope is intentionally minimal. Infrastructure fields live in the envelope; all application semantics live in `payload`.

**Ordering note:** Messages within a single channel on a single instance are totally ordered by write sequence. Across federated hubs, messages from different origins arrive in network order, not origin-timestamp order. The `ts` field reflects when the message was published at its origin and can be used for approximate ordering, but clock skew between hosts means it is not a reliable global ordering key. No reorder buffer is provided; subscribers must tolerate out-of-order delivery across hub boundaries.

### 4.2 Payload Type Registry

Publishers register a payload type string and a corresponding Go type at startup:

```go
messenger.RegisterPayloadType("com.keyop.orders.OrderCreated", OrderCreated{})
```

Subscribers receive the decoded `payload` field as the registered type. An unregistered `payload_type` is delivered as `map[string]any` with a warning logged — it is never dropped.

### 4.3 Envelope Versioning

The `v` field allows future changes to the envelope schema. Readers must handle unknown `v` values gracefully (log and skip, or pass through raw). Version `1` is the only defined version.

---

## 5. Storage Layer

### 5.1 File Layout

Each channel maps to a single append-only file:

```
{data_dir}/
  channels/
    orders.jsonl
    orders.dead-letter.jsonl
    payments.jsonl
    alerts.jsonl
  subscribers/
    orders/
      {subscriber-id}.offset
    payments/
      {subscriber-id}.offset
  audit.jsonl
  audit.jsonl.1
  audit.jsonl.2
```

One file per channel avoids cross-channel filtering on read and allows per-channel retention policies. Dead-letter channels follow the same file layout as regular channels.

### 5.2 Append Atomicity

All writes to a `.jsonl` file are serialized through a single writer goroutine per channel. `Publish()` sends a write request to the writer goroutine and then **blocks until the writer confirms the write is complete**. There is no in-memory queue between the publisher and the writer — a message is not considered published until it has been handed to the OS. This ensures no messages are silently discarded if the process restarts while work is pending.

Multiple goroutines calling `Publish()` concurrently on the same channel serialize through the writer goroutine; each waits for its own write to be confirmed before returning.

The writer appends with `O_APPEND` and issues a single `write()` syscall per record, which is atomic for records under `PIPE_BUF` on POSIX systems. For larger records, the writer holds an advisory `flock` for the duration of the write.

What "write confirmed" means depends on the sync policy:

| Sync policy | `Publish()` returns after | Durable against |
|---|---|---|
| `none` | `write()` syscall returns | Application restart (data in OS page cache) |
| `periodic` | `write()` syscall returns; fsync runs on a background timer | Application restart; OS crash only between fsync intervals |
| `always` | `fsync()` completes | OS crash, power failure |

### 5.3 Backpressure on Disk Full

If the channel writer goroutine encounters a write error (disk full, I/O error), it does **not** drop the message or return an error to the caller. Instead:

1. The write is retried on a short interval until it succeeds.
2. The writer goroutine's inbox channel fills up because no new records are being consumed.
3. Once full, `Publish()` blocks the calling goroutine until the writer can accept the record.
4. For federated messages arriving over WebSocket: the hub's per-peer receiver goroutine blocks on `Publish()`, which stops it from reading further WebSocket frames. TCP flow control propagates this backpressure to the sending peer's OS send buffer, which in turn blocks the sending goroutine on the remote hub.

This guarantees that a full disk causes the entire affected write path to stall rather than silently lose messages. The operator must resolve the disk condition; no messages are dropped.

### 5.4 Subscriber Offset Tracking

Each subscriber has a durable offset file recording the byte offset of the next unread record in the channel file. The offset is written **after** the subscriber's handler returns successfully — this is the at-least-once contract.

If the process crashes after processing but before writing the offset, the message is redelivered on restart. Subscriber handlers must be idempotent.

The offset file is fsynced on every update. This is not configurable — relaxing this guarantee would break at-least-once semantics.

### 5.5 Dead-Letter Queue

When a subscriber's handler returns an error or panics, the message is retried up to a configurable maximum (`max_retries`, default: 5). Retries are immediate — there is no delay between attempts within the same delivery cycle. On the final retry failure:

1. The original message is wrapped in a dead-letter envelope and published to `{channel}.dead-letter`.
2. The subscriber's offset is advanced past the failed message so delivery continues.
3. The dead-letter event is logged at error level.

The dead-letter envelope payload:

```json
{
  "original":     { ... original envelope ... },
  "retries":      5,
  "last_error":   "handler returned: invalid state transition",
  "failed_at":    "2026-03-31T14:22:05.000000000Z"
}
```

Dead-letter channels are regular channels. They can be subscribed to for monitoring or reprocessing. Dead-letter channels are not themselves subject to dead-lettering (a failing dead-letter handler is logged and the offset advanced).

### 5.6 File Change Notification

Subscribers use `fsnotify` (inotify on Linux, kqueue on macOS) to receive push notification when new data is appended to a channel file. On notification, the subscriber reads from its current offset to EOF in a single buffered read.

Polling is not used in normal operation. If `fsnotify` is unavailable for a channel, the subscriber falls back to polling with a 100ms interval and logs a warning.

### 5.7 File Rotation and Compaction

A channel file may not be truncated or deleted while any subscriber's offset is less than the file's current size. The compaction process:

1. Reads the minimum offset across all registered subscribers for a channel.
2. If the minimum offset exceeds a configurable threshold (e.g., 256 MB), rotates the file: the consumed portion is archived or deleted, and offsets are adjusted.
3. Rotation is performed under a brief write pause (the writer goroutine drains its inbox and holds the lock while rotation completes).

A subscriber that falls too far behind (configurable max lag in bytes) is logged as a warning. No automatic action is taken — operational policy determines whether to wait, alert, or drop the lagging subscriber.

### 5.8 Subscriber Registration

Subscribers must be explicitly registered before consuming. Registration writes an initial offset file (at the current end-of-file for new subscribers, or reads the existing offset for resuming subscribers). The compaction process uses the registered subscriber list to determine the safe deletion boundary.

Deregistering a subscriber removes its offset file and allows compaction to proceed past that subscriber's last position.

---

## 6. Federation

### 6.1 Instance Identity

Each instance is identified by a human-readable **instance name**, which defaults to the OS hostname. If multiple instances run on the same physical host (e.g., on different ports), the name should be set explicitly in config to `hostname:port` to ensure uniqueness.

The instance name is embedded in the TLS certificate as the Common Name and as a DNS Subject Alternative Name. It is used for:
- Allowlist authorization (hub checks connecting instance name against its config)
- The `origin` field in message envelopes
- Audit log entries
- Human-readable log messages

The message `id` field (UUID v4) is separate from the instance name. It is generated per-message and used exclusively for deduplication. It is not an instance identifier.

### 6.2 WebSocket Connection

Federation uses WebSocket over TLS (`wss://`). The TLS configuration requires TLS 1.3 minimum. Both sides present certificates; both sides verify the peer cert against the shared CA.

After the TLS handshake, the application-level handshake exchanges:

```json
{
  "instance_name": "billing-host",
  "role":          "hub",
  "version":       1
}
```

The `role` field is informational. Authorization (see §6.4) determines whether the connection is accepted regardless of the declared role.

### 6.3 Message Wire Format

Messages are sent as binary WebSocket frames. Each frame carries one or more envelope records, length-prefixed, to allow batching:

```
[4 bytes: record length][record bytes][4 bytes: record length][record bytes]...
```

Batching is applied when messages are queued faster than the network can drain them. The sender flushes the current batch immediately if the queue is empty (no artificial delay). Maximum batch size is configurable (default: 64 KB per frame).

An acknowledgment frame is sent by the receiver after writing a batch to its local `.jsonl` file. The sender buffers unacknowledged messages and retransmits on reconnect.

### 6.4 Authorization: Two Layers

**Layer 1 — mTLS:** The TLS handshake verifies the peer holds a certificate signed by the configured CA. A peer with no valid cert or a cert from an unknown CA is rejected at the TLS layer before any application code runs.

**Layer 2 — Allowlist:** After the handshake, the hub extracts the instance name from the peer cert's CN and checks it against its configured `allowed_clients` or `peer_hubs` list. A peer with a valid cert but an unrecognized name is rejected with a close frame (code 4403) and the connection is recorded in the audit log.

Clients never perform allowlist checks — a client accepts any hub it is configured to dial (trust is established by the cert).

**Allowlist changes on hot-reload:** When a client's name is removed from the allowlist during a policy reload, the existing connection is allowed to drain in-flight messages before being closed gracefully. The connection is not dropped mid-message.

### 6.5 Hub-to-Hub Forwarding Policy

Forwarding policy is configured statically per peer hub connection. There are no wildcards; channel names are exact strings.

```yaml
peer_hubs:
  - addr: "hub2.external:7740"
    forward:   ["alerts", "public-events"]   # channels this hub sends to hub2.external
    receive:   ["ack", "external-status"]    # channels this hub accepts from hub2.external
```

The peer hub is identified by the hostname in `addr`. The hub verifies that the connecting peer's cert CN matches this hostname.

**Forward policy (outbound):** When a message arrives on a channel in `forward`, the hub enqueues it for transmission to the peer hub. Messages on channels not in `forward` are never sent to that peer.

**Receive policy (inbound):** When a message arrives from a peer hub, the hub checks `receive`. If the channel is not in `receive`, the message is discarded and recorded in the audit log as a policy violation. This is defense-in-depth — it protects against peer misconfiguration.

Forwarding is independent of whether any local subscriber currently exists for the channel. Messages are written to the local `.jsonl` file regardless of local subscriber presence.

### 6.6 Policy Hot-Reload

The hub watches its configuration file using `fsnotify`. When the file changes:

1. The new configuration is parsed and validated. If invalid, the reload is aborted and an error is logged; the existing policy remains active.
2. For each existing peer hub connection, the forward/receive channel lists are atomically swapped.
3. New peer hubs in the config are dialed. Removed peer hubs have their connections closed gracefully after draining in-flight messages.
4. Client allowlist additions take effect immediately for new connections. Removals allow existing connections to drain before closing — no in-progress delivery is interrupted.

Policy reload does not restart the hub, drop existing connections mid-message, or interrupt local message delivery. A `policy_reloaded` event is written to the audit log on every successful reload.

### 6.7 Message Deduplication

Every instance (hub and client alike) maintains an in-memory LRU set of recently-seen message IDs. The set holds the last 100,000 IDs (configurable). TTL-based expiry is not used; the LRU eviction bound is sufficient for the expected message rates.

On receiving a message (from any source — local publish, peer hub, or reconnect replay):

1. Check the seen-ID set.
2. If present: discard silently.
3. If absent: add to set, write to local file, notify local subscribers, forward to eligible peer hubs.

This handles:
- A client connected to multiple hubs receiving the same message twice
- Hub ring topologies (Hub 1 → Hub 2 → Hub 1)
- Reconnect replay delivering messages already written in a previous session

### 6.8 Reconnection and Replay

When a peer hub connection is lost:

1. The hub begins buffering outbound messages for that peer (up to a configurable limit, default 10,000 messages). Messages beyond the buffer limit are dropped with a warning logged.
2. Reconnection is attempted with exponential backoff (base 500ms, max 60s, jitter ±20%).
3. On reconnect, the reconnecting side sends its last-acknowledged message ID. The peer replays any messages in its local `.jsonl` file with an ID after that point that match the agreed forward channels.
4. If the last-acknowledged message ID is no longer in the peer's file (compacted away), an error is logged and delivery continues from the earliest available record. This is a gap — messages compacted before the reconnecting peer could receive them are permanently missed. The gap is recorded in the audit log.

---

## 7. Security

### 7.1 Certificate Authority

A self-signed CA is generated once per cluster using a P-384 EC key. The CA cert is distributed to every instance. The CA key is kept offline or in a secrets manager — it is not needed by running instances.

Each instance holds:
- Its own certificate (signed by the CA), with its instance name as the CN and DNS SAN
- Its private key
- The CA certificate (for verifying peers)

### 7.2 Certificate Generation

A CLI tool (`keyop-messenger keygen`) generates:

```
keyop-messenger keygen ca               # generate CA cert + key

keyop-messenger keygen instance \
  --ca ca.crt --ca-key ca.key \
  --name billing-host \
  --out-cert billing-host.crt \
  --out-key  billing-host.key
```

For an instance running on a non-default port (multiple instances per host), pass the full name:

```
keyop-messenger keygen instance \
  --ca ca.crt --ca-key ca.key \
  --name billing-host:7741 \
  --out-cert billing-host-7741.crt \
  --out-key  billing-host-7741.key
```

Generated certs have a configurable validity period (default: 2 years). The messenger library logs a warning when a cert is within 30 days of expiry.

### 7.3 Certificate Rotation

Instances reload their own cert and key from disk when the files change (watched via `fsnotify`). The new cert is applied to the TLS config for new connections only. Existing connections are not renegotiated. The CA cert is reloaded the same way.

Revoking an instance: remove it from all hub allowlists (takes effect on next reload for new connections; existing connections drain before closing).

### 7.4 Local File Security

Channel `.jsonl` files and offset files should be readable only by the user running the instance (`chmod 600` or `640`). The library does not enforce filesystem permissions itself — this is a deployment concern.

---

## 8. Audit Log

All cross-hub message forwarding is recorded in `{data_dir}/audit.jsonl`. Each audit record is a JSON line:

```json
{
  "ts":         "2026-03-31T14:22:01.123456789Z",
  "event":      "forward",
  "message_id": "01952c3e-7b2a-7c4d-9f1a-3e8d2b1c0a5f",
  "channel":    "alerts",
  "direction":  "outbound",
  "peer":       "hub2.external",
  "peer_addr":  "hub2.external:7740"
}
```

Event types:

| `event` | Meaning |
|---|---|
| `forward` | Message forwarded to or received from a peer hub |
| `policy_violation` | Inbound message rejected by receive policy |
| `replay_gap` | Reconnecting peer's last-ack ID was compacted away; delivery continues from earliest available |
| `peer_connected` | A peer hub connection was established |
| `peer_disconnected` | A peer hub connection was lost |
| `client_connected` | A client connected to this hub |
| `client_rejected` | A client was rejected (name not in allowlist) |
| `client_drain` | An allowlist-removed client is draining before disconnect |
| `policy_reloaded` | Forwarding policy was hot-reloaded successfully |
| `policy_reload_failed` | Policy reload was aborted due to parse/validation error |

### 8.1 Audit Log Rotation

The audit log is rotated automatically by the library. When `audit.jsonl` reaches the configured maximum size, it is renamed to `audit.jsonl.1`, existing numbered files are shifted (`audit.jsonl.1` → `audit.jsonl.2`, etc.), and a new `audit.jsonl` is opened. Files beyond the configured `max_files` count are deleted.

Rotation is performed by the audit writer goroutine under a brief pause. No audit records are lost during rotation.

---

## 9. Performance Design

### 9.1 Write Path

```
publisher goroutine
  └─ marshal envelope to []byte
  └─ send write request to per-channel writer goroutine + wait for confirmation
       (rendezvous — Publish() blocks until writer signals done)

writer goroutine (one per channel)
  └─ receive next write request
  └─ single write() syscall
  └─ retry on I/O error until success (see §5.3)
  └─ fsync if sync_policy = "always"
  └─ signal publisher: write confirmed
  └─ signal waiting subscribers via fsnotify or internal channel
```

`Publish()` always blocks until the write is confirmed. There is no in-memory queue and no separate timeout or drop path — if the disk is unavailable, the publisher waits indefinitely (see §5.3 for backpressure propagation to federated senders).

### 9.2 Read Path

```
fsnotify event (or internal signal for same-process subscribers)
  └─ subscriber goroutine wakes
  └─ read from current offset to EOF in one buffered read
  └─ split on newlines, unmarshal each record
  └─ dispatch to handler (with retry loop up to max_retries)
  └─ on final retry failure: publish to dead-letter channel, advance offset
  └─ write offset file after each successful handler return or dead-letter
```

Subscribers within the same process use an internal notification channel bypassing the filesystem watcher for lower latency.

### 9.3 Serialization

JSON is used throughout for debuggability. To minimize allocation:
- The envelope is pre-marshaled by the publisher using `encoding/json` with a preallocated buffer from `sync.Pool`.
- Subscriber reads use a streaming decoder that reuses its internal buffer.
- Payload type registration uses pre-generated codec functions where possible (e.g., via `easyjson` or `json-iterator`).

The wire format between hubs uses the same JSON envelope, length-prefixed within a binary WebSocket frame.

### 9.4 Concurrency Model

| Component | Goroutine model |
|---|---|
| Per-channel file writer | 1 goroutine per channel, owns the file descriptor |
| Per-subscriber reader | 1 goroutine per subscriber, independent cursor |
| Per-peer-hub sender | 1 goroutine per peer connection, owns the send queue |
| Per-peer-hub receiver | 1 goroutine per peer connection |
| Policy watcher | 1 goroutine, shared |
| Audit writer | 1 goroutine, shared |

No shared mutable state is accessed without synchronization. Per-channel and per-peer goroutines communicate via channels, not mutexes, except for the seen-ID LRU (protected by a read-write mutex).

---

## 10. Configuration Reference

```yaml
# Full configuration with all fields and defaults

name: ""                   # Instance name. Defaults to OS hostname. Use "hostname:port"
                           # if multiple instances share a host.

storage:
  data_dir: "/var/keyop"   # Required. Root directory for channel files, offset files, audit log.
  sync_policy: "periodic"  # "none" | "periodic" | "always"
  sync_interval_ms: 200    # Used when sync_policy = "periodic"
  max_subscriber_lag_mb: 512   # Warn when a subscriber is this far behind
  compaction_threshold_mb: 256 # Rotate channel file when consumed portion exceeds this size

subscribers:
  max_retries: 5           # Retry count before routing a message to the dead-letter channel

hub:
  enabled: false
  listen_addr: ""          # e.g. "0.0.0.0:7740"

  allowed_clients:         # Instance names permitted to connect as clients
    - name: "billing-host"
    - name: "orders-host"

  peer_hubs:               # Peer hubs to connect to
    - addr: ""             # host:port — hostname must match peer cert CN
      forward: []          # Channels to send to this peer hub (exact names, no wildcards)
      receive: []          # Channels to accept from this peer hub (exact names, no wildcards)

client:
  enabled: false
  hubs:                    # Hubs to dial (used when this instance is a client)
    - addr: ""             # host:port

tls:
  cert: ""                 # Path to instance certificate (PEM)
  key:  ""                 # Path to instance private key (PEM)
  ca:   ""                 # Path to CA certificate (PEM)
  min_version: "1.3"       # Minimum TLS version
  expiry_warn_days: 30     # Log a warning this many days before cert expiry

federation:
  reconnect_base_ms: 500
  reconnect_max_ms: 60000
  reconnect_jitter: 0.2
  send_buffer_messages: 10000   # Max buffered messages per peer during disconnect
  max_batch_bytes: 65536        # Max WebSocket frame payload size

dedup:
  seen_id_lru_size: 100000

audit:
  max_size_mb: 100         # Rotate audit.jsonl when it reaches this size
  max_files: 10            # Number of rotated audit files to retain
```
