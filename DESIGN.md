# Keyop Messenger — Design Document

## 1. Overview

Keyop Messenger is a pub-sub messaging library for distributed Go applications. It provides:

- **Reliable at-least-once delivery** backed by append-only `.jsonl` log files
- **Fan-out isolation** — slow subscribers do not affect other subscribers or publishers
- **Federated messaging** — instances connect via WebSocket with mutual TLS; hubs forward select channels to peer hubs under explicit policy
- **Subscription-based routing** — clients declare which channels they want; hubs enforce per-client allowlists that are statically configured and hot-reloaded without restart
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
Instance A ─┐                         Instance D ─┐
Instance B ─┼──→ Hub 1 ──(selected)──→ Hub 2 ←────┤─ Instance E
Instance C ─┘           channels only             └──(selected)──→ Instance F
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

Each channel is a directory containing one or more segment files:

```
{data_dir}/
  channels/
    orders/
      00000000000000000000.jsonl   # segment 0 (sealed)
      00000000000000065536.jsonl   # segment 1 (sealed)
      00000000000000131072.jsonl   # segment 2 (active — writer appends here)
    orders.dead-letter/
      00000000000000000000.jsonl
    payments/
      00000000000000000000.jsonl
  subscribers/
    orders/
      {subscriber-id}.offset
    payments/
      {subscriber-id}.offset
  audit/
    audit.jsonl
    audit.jsonl.1                  # rotated (most recent)
    audit.jsonl.2                  # rotated (older)
```

Each channel is a directory containing one or more segment files. Segment filenames encode the global byte offset at which that segment begins, zero-padded to 20 digits so lexicographic order equals offset order. The active segment is always the file with the highest start offset; all others are sealed. Dead-letter channels follow the same directory layout as regular channels.

### 5.2 Append Atomicity

All writes to a `.jsonl` file are serialized through a single writer goroutine per channel. `Publish()` hands a write request to the writer goroutine via an **unbuffered** channel (rendezvous) and then **blocks until the writer confirms the write is complete**. There is no in-memory queue — a message is not considered published until it has been handed to the OS. This ensures no messages are silently discarded if the process restarts while work is pending.

Multiple goroutines calling `Publish()` concurrently on the same channel serialize through the writer goroutine; each blocks on the rendezvous until its own write is confirmed before returning.

The writer appends with `O_APPEND` and issues a single `write()` syscall per record, which is atomic for records under `PIPE_BUF` on POSIX systems. For larger records, the writer holds an advisory `flock` for the duration of the write.

What "write confirmed" means depends on the sync policy:

| Sync policy | `Publish()` returns after | Durable against |
|---|---|---|
| `none` | `write()` syscall returns | Application restart (data in OS page cache) |
| `periodic` | `write()` syscall returns; fsync runs on a background timer | Application restart; OS crash only between fsync intervals |
| `always` | `fsync()` completes | OS crash, power failure |

For `sync_policy=always`, an fsync failure is returned as an error to the caller. For `sync_policy=periodic`, fsync failures are logged as warnings and retried on the next timer tick; `Publish()` is not affected.

### 5.3 Backpressure on Disk Full

If the channel writer goroutine encounters a write error (disk full, I/O error), it does **not** drop the message or return an error to the caller. Instead:

1. The write is retried every 10ms until it succeeds.
2. Because the writer goroutine is busy in the retry loop, it is not reading from the (unbuffered) rendezvous channel. Any concurrent `Publish()` call blocks immediately when it tries to hand its request to the writer.
3. For federated messages arriving over WebSocket: the hub's per-peer receiver goroutine blocks on `Publish()`, which stops it from reading further WebSocket frames. TCP flow control propagates this backpressure to the sending peer's OS send buffer, which in turn blocks the sending goroutine on the remote hub.

This guarantees that a full disk causes the entire affected write path to stall rather than silently lose messages. The operator must resolve the disk condition; no messages are dropped.

### 5.4 Subscriber Offset Tracking

Each subscriber has a durable offset file recording the **global byte offset** of the next unread record in the channel's logical stream. The offset is written **after** the subscriber's handler returns successfully — this is the at-least-once contract.

A new subscriber starts at the global offset of the stream end (last segment's start offset + last segment's size), skipping pre-existing history. A restarting subscriber resumes from the persisted offset.

To find the right segment given a global offset: iterate the sorted segment list and find the segment where `seg.startOffset <= globalOffset < nextSeg.startOffset`. Seek within the file to `globalOffset - seg.startOffset`.

If the process crashes after processing but before writing the offset, the message is redelivered on restart. Subscriber handlers must be idempotent.

Offset files are written atomically: the new value is written to a `.tmp` sibling file, fsynced, then renamed over the real file. This ensures the offset file is never left in a half-written state. The fsync is not configurable — relaxing it would break at-least-once semantics.

**Persistent offset write failures:** If `WriteOffset` fails (e.g., the offset partition is full), the in-memory offset is **not** advanced — the conservative choice that ensures the message will be re-delivered on the next restart rather than silently skipped. The error is logged. After three consecutive failures, delivery is **paused**: the subscriber goroutine continues running and listening for change notifications, but on each wakeup it first attempts a probe write of the current offset. If the probe succeeds, the failure counter is reset and normal delivery resumes. If it fails, delivery remains paused and the subscriber waits for the next notification before trying again. This prevents unbounded duplicate delivery when the offset partition is persistently unavailable.

### 5.5 Dead-Letter Queue

When a subscriber's handler returns an error or panics, the message is retried up to a configurable maximum (`max_retries`, default: 5). Between retry attempts the subscriber observes an **exponential backoff** delay: the first retry waits 100ms, doubling on each subsequent attempt and capping at 5s. If `Stop()` is called during a backoff sleep the subscriber exits immediately without advancing the offset — the message will be re-delivered on the next start. On the final retry failure:

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

**Same-process fast path:** Subscribers in the same process as the publisher receive notifications via a `LocalNotifier` — a capacity-1 `chan struct{}` that the writer goroutine signals directly after each successful write. This bypasses the filesystem watcher entirely for lower latency.

**Path normalisation:** All watched paths are resolved to their absolute form via `filepath.Abs` before registration. This ensures that a relative path and an absolute path referring to the same file share a single watch entry rather than creating duplicate goroutines.

**Polling fallback:** A 100ms polling goroutine is started per path in two situations:
1. `fsnotify.Add` fails at `Watch` time (e.g., inotify watch limit reached).
2. A runtime error is received from the fsnotify backend (e.g., inotify queue overflow). In this case, polling is started for every currently-watched path that is not already being polled.

The polling goroutine detects changes by comparing both `ModTime` **and** `Size` against their values at the previous tick. Checking size as well as mtime ensures changes are detected on filesystems with coarse mtime resolution (e.g., ext3 at 1-second granularity, some network shares) when multiple writes occur within the same second.

Multiple notifications from overlapping sources (fsnotify and polling) are coalesced: the notification channel has a capacity of 1 and uses a non-blocking send, so the subscriber sees at most one pending wake-up regardless of how many events fire.

### 5.7 File Rotation and Compaction

**Segment rolling:** The writer rolls to a new segment when the current segment's size would exceed `max_segment_bytes` (default: 64 MB). Rolling is O(1): the current segment is synced and closed, a new file is created with a name encoding its start offset, and subsequent writes land in the new file. There is no pause, copy, or coordination with subscribers.

**Compaction:** The compactor periodically scans the channel directory and deletes any sealed segment (all segments except the active one) whose entire content lies before the minimum subscriber offset — i.e., every registered subscriber has advanced past the segment's last byte. Deletion is a single `unlink` syscall. No writer pause is needed: the writer holds the active segment open, and Unix permits deletion of sealed segments even while subscribers hold open file descriptors to them (the inode persists until all readers close their fds).

A subscriber that falls too far behind (configurable `max_subscriber_lag_bytes`) is logged as a warning. No automatic action is taken.

### 5.8 Subscriber Registration

Subscribers must be explicitly registered before consuming. Registration writes an initial offset file (at the current end-of-file for new subscribers, or reads the existing offset for resuming subscribers). The compaction process uses the registered subscriber list to determine the safe deletion boundary.

Deregistering a subscriber removes its offset file and allows compaction to proceed past that subscriber's last position.

---

## 6. Federation

### 6.1 Instance Identity

Each instance is identified by a human-readable **instance name**, which defaults to the OS hostname. If multiple instances run on the same physical host (e.g., on different ports), the name should be set explicitly in config to `hostname:port` to ensure uniqueness. If the name is empty after applying defaults (e.g., `os.Hostname()` fails), startup is rejected with a validation error.

The instance name is embedded in the TLS certificate as the Common Name and as a DNS Subject Alternative Name. It is used for:
- Allowlist authorization (hub checks connecting instance name against its config)
- The `origin` field in message envelopes
- Audit log entries
- Human-readable log messages

The message `id` field (UUID v4) is separate from the instance name. It is generated per-message and used exclusively for deduplication. It is not an instance identifier.

### 6.2 WebSocket Connection

Federation uses WebSocket over TLS (`wss://`). Both sides present certificates; both sides verify the peer cert against the shared CA. The `tls.min_version` config field accepts only `"1.2"` or `"1.3"` and defaults to `"1.3"`; any other value is rejected at startup.

After the TLS handshake, the application-level handshake exchanges a JSON text frame. The connecting side sends first:

```json
{
  "instance_name": "billing-host",
  "role":          "client",
  "version":       "1",
  "last_id":       "01952c3e-...",
  "subscribe":     ["alerts", "metrics"]
}
```

The hub responds with its own handshake (without a `subscribe` field):

```json
{
  "instance_name": "hub1",
  "role":          "hub",
  "version":       "1"
}
```

The `role` field is informational. Authorization (see §6.4) determines whether the connection is accepted regardless of the declared role. `last_id` is omitted on first connection; `subscribe` is omitted when the client only publishes and does not want to receive any channels.

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

### 6.5 Subscription Model and Channel Policy

#### 6.5.1 Client Subscriptions

Clients declare which channels they want to receive in the `subscribe` field of the handshake. The hub computes the effective channel set as:

```
effective = client.subscribe ∩ hub_allowlist_for(client)
```

The hub creates an outbound sender for the client only when the effective set is non-empty. A client that sends no `subscribe` list (or an empty one) will not receive any messages from the hub regardless of what the hub config says.

There are no wildcards; all channel names are exact strings.

#### 6.5.2 Hub Access Control for Clients

Each entry in `allowed_clients` may optionally list the channels that client is permitted to subscribe to:

```yaml
hub:
  allowed_clients:
    - name: "billing-host"
      allow_channels: ["metrics", "alerts"]  # may only subscribe to these
    - name: "monitor"
      allow_channels: []                     # empty = permit any channel
```

An empty `allow_channels` list means the client may subscribe to any channel. A non-empty list acts as an allowlist; the hub silently restricts the effective subscription to the intersection.

#### 6.5.3 Peer Hub Policy

Peer hubs connect to this hub using the same subscription mechanism. The `forward` list on a `peer_hubs` entry acts as the channel allowlist for that peer hub — it controls what that peer is permitted to subscribe to (not an auto-push list):

```yaml
hub:
  peer_hubs:
    - addr: "hub2.external:7740"
      forward: ["alerts", "public-events"]   # channels hub2.external is allowed to subscribe to
      receive: ["ack", "external-status"]    # channels this hub accepts inbound from hub2.external
```

The peer hub is identified by the hostname in `addr`. The hub verifies that the connecting peer's cert CN matches this hostname.

When `hub2.external` connects, it sends `subscribe: ["alerts"]` in its handshake. The hub computes `effective = {"alerts"} ∩ {"alerts", "public-events"} = {"alerts"}` and creates a sender for that channel only.

On the client side (`hub2.external`'s config), the `subscribe` field on a `client.hubs` entry declares what channels to request:

```yaml
client:
  enabled: true
  hubs:
    - addr: "hub1.internal:7740"
      subscribe: ["alerts"]
```

#### 6.5.4 Receive Policy (Inbound Filter)

The `receive` list on a `peer_hubs` entry is an inbound filter that controls which channels this hub will accept when the peer hub sends messages to it. If the channel is not in `receive`, the message is discarded and recorded in the audit log as a policy violation. This is a defense-in-depth measure that protects against peer misconfiguration.

An **empty `receive` list** means "accept all channels from this peer" — no inbound filter is applied. Regular client connections always have an empty receive policy.

#### 6.5.5 General Notes

Forwarding is independent of whether any local subscriber currently exists for the channel. Messages are written to the local `.jsonl` file regardless of local subscriber presence.

### 6.6 Policy Hot-Reload

The hub watches its configuration file using `fsnotify`. When the file changes:

1. The new configuration is parsed and validated. If invalid, the reload is aborted and an error is logged; the existing policy remains active.
2. For each existing peer connection, the effective channel set is recomputed as `stored_subscribe ∩ new_allowlist` and swapped atomically. This means admin changes to `allow_channels` or `forward` take effect immediately for in-flight connections without requiring reconnection. If the new allowlist removes a channel that a peer subscribed to, the hub stops sending that channel to that peer immediately.
3. Peers whose names have been removed from the allowlist have their connections drained gracefully before closing.
4. Client allowlist additions take effect immediately for new connections. Removals allow existing connections to drain before closing — no in-progress delivery is interrupted.

**Note on allowlist expansion:** If an admin adds new channels to a peer's allowlist after the peer has already connected, the peer will not automatically receive those new channels — it can only receive channels it originally subscribed to at connect time. The peer must reconnect and include the new channels in its `subscribe` list to pick them up.

Policy reload does not restart the hub, drop existing connections mid-message, or interrupt local message delivery. A `policy_reloaded` event is written to the audit log on every successful reload.

### 6.7 Message Deduplication

Every instance (hub and client alike) maintains an in-memory LRU set of recently-seen message IDs. The set holds the last 100,000 IDs (configurable). TTL-based expiry is not used; the LRU eviction bound is sufficient for the expected message rates.

On receiving a message via federation (PeerReceiver):

1. Check the seen-ID set.
2. If present: discard silently.
3. If absent: add to set, write to local file, notify local subscribers.

On `Publish()` (local origin):

1. Add the message ID to the seen-ID set before writing to disk.
2. If the message later arrives back via a federation path (e.g., because a peer hub reflects it), step 2 above catches it.

The dedup set is shared across all PeerReceivers on the same Messenger instance. This ensures that if the same envelope arrives via two simultaneous incoming connections (dual-path forwarding), only the first arrival triggers a local write.

**Note on ring topologies:** Received federated messages are written to local storage by `writeLocalEnvelope` but are not automatically re-enqueued for outbound forwarding. This means Hub1 → Hub2 → Hub1 loops cannot form with the current implementation. The dedup is in place as a defence-in-depth measure: if forwarding semantics change in the future such that received messages can be re-sent, the LRU set will prevent delivery loops and duplicate local writes.

Practical scenarios the dedup handles today:
- Two client connections from the same publisher both deliver the same envelope to a hub (dual-path test)
- A publisher's own message arrives back via a federated receive path (self-loop prevention)
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

To rotate a certificate, replace the cert and key files on disk and restart the instance. The new cert is loaded at startup; existing long-lived connections from before the restart are replaced by fresh connections that use the new cert.

Revoking an instance: remove it from all hub allowlists (takes effect on next policy reload for new connections; existing connections drain before closing).

**Note:** Automatic hot-reload of TLS certificates (without restart) is not currently implemented. The library validates certificate expiry at startup and logs a warning when a cert is within `tls.expiry_warn_days` of expiry.

### 7.4 Local File Security

Channel `.jsonl` files and offset files should be readable only by the user running the instance (`chmod 600` or `640`). The library does not enforce filesystem permissions itself — this is a deployment concern.

---

## 8. Audit Log

All cross-hub message forwarding is recorded in `{data_dir}/audit/audit.jsonl`. Each audit record is a JSON line:

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

### 8.2 Backpressure and Drop Behavior

The audit writer uses an internal channel (capacity 1,000) to decouple callers from disk I/O. If the channel is full, the event is dropped rather than blocking the caller — audit logging must never impede message delivery.

Drops are surfaced via two mechanisms:

1. **Immediate:** a message is written to `stderr` for each dropped event.
2. **Periodic (structured):** the audit writer goroutine tracks a running drop counter atomically. Every 5 seconds, if the counter is non-zero, it is swapped to zero and a `WARN`-level entry is emitted through the injected `Logger` interface (which feeds into the operator's log aggregation pipeline) with the count of drops since the last report. Any remaining drops are also reported on `Close()`.

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
fsnotify event on channel directory (or internal signal for same-process)
  └─ subscriber goroutine wakes
  └─ list segment files in channel directory (sorted by start offset)
  └─ for each segment starting at or after subscriber's global offset:
       └─ open segment, seek to (globalOffset - segmentStartOffset)
       └─ scan lines to EOF; dispatch each with retry + backoff
       └─ advance to next segment's start offset when current segment EOF reached
  └─ write global offset file after each successful dispatch or dead-letter
       → on write failure: log error, leave in-memory offset unchanged
```

Subscribers within the same process use an internal notification channel bypassing the filesystem watcher for lower latency. The per-line scanner buffer is capped at 10 MiB; envelopes larger than this are skipped with an error logged and the offset advanced past them.

### 9.3 Serialization

JSON is used throughout for debuggability. The standard library `encoding/json` is the only serialization dependency. To minimise allocation:
- The envelope is marshaled by the publisher with a preallocated byte-slice buffer.
- Subscriber reads use `bufio.Scanner` which reuses its internal buffer across lines.
- The payload registry stores a prototype value per type string; `json.Unmarshal` decodes into a fresh copy of that prototype on each message delivery.

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
  max_segment_bytes: 67108864  # 64 MiB; writer rolls to new segment when active segment reaches this size

subscribers:
  max_retries: 5           # Retry count before routing a message to the dead-letter channel.
                           # Omitting the field (nil) applies the default of 5.
                           # Setting it explicitly to 0 routes to dead-letter immediately on
                           # the first failure with no retries.

hub:
  enabled: false
  listen_addr: ""          # e.g. "0.0.0.0:7740"

  allowed_clients:         # Instance names permitted to connect as clients
    - name: "billing-host"
      allow_channels: []   # Channels this client may subscribe to; empty = any channel
    - name: "orders-host"
      allow_channels: ["metrics", "alerts"]

  peer_hubs:               # Peer hub connections (peers that connect TO this hub)
    - addr: ""             # host:port — hostname must match peer cert CN
      forward: []          # Allowlist: channels this peer hub may subscribe to (exact names, no wildcards)
      receive: []          # Inbound filter: channels accepted from this peer hub (empty = any channel)

client:
  enabled: false
  hubs:                    # Hubs to dial (used when this instance is a client or peer hub)
    - addr: ""             # host:port
      subscribe: []        # Channels to request from this hub (exact names, no wildcards)

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

---

## 11. Ephemeral Client

### 11.1 Overview

An **ephemeral client** is a process that connects to a hub without maintaining any local storage. It has two primary use cases:

- **Fire-and-forget publishers** that need guaranteed delivery confirmation (the hub has written the message to disk) but do not consume messages themselves.
- **Live-view consumers** that want to receive messages only while connected; they accept that messages published during a disconnect are permanently missed.

The public API is `EphemeralMessenger`; the internal implementation is `EphemeralClient` in `internal/federation`.

### 11.2 Handshake Flag

The ephemeral client sets `"ephemeral": true` in its handshake frame:

```json
{
  "instance_name": "billing-service",
  "role":          "client",
  "version":       "1",
  "subscribe":     ["alerts"],
  "ephemeral":     true
}
```

The `ephemeral` field is `omitempty`; it is absent from non-ephemeral connections and from the hub's response. This ensures backward compatibility with hubs that do not yet understand the field.

When the hub sees `ephemeral: true`, it skips the `ReplayFrom` step on connect — even if `last_id` is set. The hub also logs ephemeral connections at a distinct info message so operators can distinguish them from durable client connections.

### 11.3 Publish Semantics

`EphemeralMessenger.Publish` (and the underlying `EphemeralClient.Publish`) blocks until one of:

| Outcome | Return value |
|---|---|
| Hub writes the message and sends an ack text frame | `nil` |
| Connection drops before the ack arrives | `ErrEphemeralConnLost` |
| `ctx` is cancelled or deadline exceeded | wrapped `ctx.Err()` |
| `Close()` was called | `ErrEphemeralClosed` |

On `ErrEphemeralConnLost`, the message **may or may not** have been received by the hub — the caller must decide whether to retry. There is no automatic retransmission; that is the caller's responsibility.

Multiple `Publish` calls may be batched into a single binary WebSocket frame when they arrive concurrently. The hub acks the entire batch with one text frame; all callers in the batch are unblocked simultaneously. The hub uses the standard `PeerReceiver` ack path, so all deduplication, policy enforcement, and local storage semantics are identical to regular federation.

### 11.4 Subscribe Semantics

`EphemeralMessenger.Subscribe` registers an in-memory handler. No offset file is created and no replay occurs:

- The handler is called synchronously in the receive goroutine; it must not block.
- On reconnect (with `AutoReconnect`), delivery resumes from the current hub position. Messages published while disconnected are never delivered.
- The channel list declared in `Subscribe []string` is sent in the handshake; the hub intersects it with its per-client allowlist as normal (`effective = subscribe ∩ allowlist`).

Handler errors are logged at `WARN` level and do not stop delivery to subsequent handlers or messages.

### 11.5 Auto-Reconnect

With `AutoReconnect: true`, `EphemeralMessenger.Connect` (which delegates to `EphemeralClient.ConnectWithReconnect`) returns after the first connection succeeds and starts a background reconnect loop. On disconnect, the loop retries with exponential backoff:

```
delay = min(reconnect_base * 2^attempt, reconnect_max) ± jitter
```

The `writeQ` channel (depth 256 by default) persists across reconnects. Items enqueued by `Publish` while disconnected wait in `writeQ` until a new connection is established. If the queue fills, new `Publish` calls block until space is available (no drops).

With `AutoReconnect: false`, `Connect` dials once. After disconnect, subsequent `Publish` calls return `ErrEphemeralConnLost` until `Connect` is called again.

### 11.6 Goroutine Model

Each call to `startConn` (internal) starts three goroutines per connection:

| Goroutine | Role |
|---|---|
| `PeerReceiver` | Owns all reads on the WebSocket conn; dispatches inbound binary frames to in-memory handlers; routes text-frame acks to `ackCh` |
| Watcher | Waits for `PeerReceiver.Done()` or `stop`; closes `connDead`, closes `conn`, then waits for `PeerReceiver` to fully exit |
| Write loop | Drains `writeQ`, writes batched binary frames, blocks waiting for an ack on `ackCh`; exits on `connDead` or `stop` |

With `AutoReconnect: true`, a fourth goroutine (`reconnectLoop`) runs for the lifetime of the client.

All goroutines are tracked in `EphemeralClient.wg`; `Close()` closes the `stop` channel and calls `wg.Wait()`, ensuring clean shutdown with no dangling goroutines.

### 11.7 No Audit Log

`EphemeralClient` does not write an audit log. Inbound messages dispatched to handlers use a `noopAuditLogger`. The hub's own audit log records ephemeral client connections (`client_connected`) and disconnections (`peer_disconnected`) via its standard `serveConn` path.

### 11.8 Differences from Regular Client

| Aspect | Regular `Client` | `EphemeralClient` |
|---|---|---|
| Local storage | Writes received messages to `.jsonl` | None — in-memory handlers only |
| Replay on reconnect | Sends `last_id`; hub replays missed messages | Never replays; `ephemeral: true` in handshake |
| Publish ack | Ack tracked per batch, unacked messages replayed on reconnect | Ack unblocks callers; on loss caller receives `ErrEphemeralConnLost` |
| Subscriber offset | Durable offset file per subscriber | No offset file |
| Audit log | Full hub events | No audit log on client side |
| Dedup | Shared instance-wide LRU (100k IDs) | Small per-connection LRU (1024 IDs, defense-in-depth) |
