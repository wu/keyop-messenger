# Keyop Messenger — Design Document

## 1. Overview

Keyop Messenger is a pub-sub messaging library for distributed Go applications. It provides:

- **Reliable at-least-once delivery** backed by append-only `.jsonl` log files
- **Fan-out isolation** — slow subscribers do not affect other subscribers or publishers
- **Federated messaging** — instances connect via gRPC with mutual TLS; hubs forward select channels to peer hubs under explicit policy
- **Subscription-based routing** — clients declare which channels they want; hubs enforce per-client allowlists that are statically configured
- **Symmetric disk-backed delivery** — both inbound (hub→peer) and outbound (publisher→hub) traffic is driven from the same per-channel segment files, with per-peer byte offsets persisted to disk; disconnects of any length never lose locally-published messages
- **Dead letter queue** — messages that exceed the retry limit are moved to a dead-letter channel rather than blocking delivery
- **Audit logging** — all cross-hub message forwarding is recorded with automatic rotation

---

## 2. Concepts

| Term                    | Definition                                                                                                                                |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| **Instance**            | A single running process embedding the messenger library                                                                                  |
| **Instance name**       | Human-readable identifier for an instance. Not configurable. When federation is enabled, derived from the local TLS certificate's Common Name (TLS is mandatory in that case). For local-only instances with no TLS, falls back to the OS hostname. Identity is independent of network location: certificates are validated by CA chain only, not by DNS-name / SAN matching, so any cert may be presented at any reachable address. Distinct instances simply need distinct CNs (e.g. `billing-host`, `orders-host`). |
| **Channel**             | A named, ordered stream of messages (analogous to a Kafka topic)                                                                          |
| **Publisher**           | Code that appends a message to a channel                                                                                                  |
| **Subscriber**          | Code that reads and processes messages from a channel                                                                                     |
| **Hub**                 | An instance that accepts inbound gRPC connections from clients and/or peer hubs                                                           |
| **Client**              | An instance that dials outbound to one or more hubs                                                                                       |
| **Peer hub**            | A hub-to-hub gRPC connection carrying forwarded messages                                                                                  |
| **Dead-letter channel** | A channel named `{channel}.dead-letter` that receives messages the subscriber has failed to process after the maximum retry count         |

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

| Field | Type | Description                                                                                                    |
|---|---|----------------------------------------------------------------------------------------------------------------|
| `v` | int | Envelope schema version. Currently `1`.                                                                        |
| `id` | string | UUID v7 assigned by the publisher. Globally unique. Used for deduplication only — not the instance identifier. |
| `ts` | string | RFC3339Nano timestamp at time of publish, in UTC.                                                              |
| `channel` | string | The channel this message was published to.                                                                     |
| `origin` | string | Instance name of the original publisher. Preserved across hub forwarding.                                      |
| `payload_type` | string | Fully-qualified type discriminator for the payload. Reverse-DNS format recommended.                            |
| `payload` | object | Arbitrary JSON object. Shape is defined by the payload type.                                                   |

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

What "write confirmed" means depends on `sync_interval_ms`:

| `sync_interval_ms` | `Publish()` returns after                                                    | Durable against                                            |
|--------------------|------------------------------------------------------------------------------|------------------------------------------------------------|
| `0`                | `fsync()` completes                                                          | OS crash, power failure                                    |
| `> 0`              | `write()` syscall returns; fsync runs on a background timer at that interval | Application restart; OS crash only between fsync intervals |

For `sync_interval_ms=0`, an fsync failure is returned as an error to the caller. For `sync_interval_ms>0`, fsync failures are logged as warnings and retried on the next timer tick; `Publish()` is not affected.

### 5.3 Backpressure on Disk Full

If the channel writer goroutine encounters a write error (disk full, I/O error), it does **not** drop the message or return an error to the caller. Instead:

1. The write is retried every 10ms until it succeeds.
2. Because the writer goroutine is busy in the retry loop, it is not reading from the (unbuffered) rendezvous channel. Any concurrent `Publish()` call blocks immediately when it tries to hand its request to the writer.
3. For federated messages arriving over gRPC: the hub's per-peer receiver goroutine blocks on `Publish()`, which stops it from reading further gRPC stream frames. HTTP/2 flow control propagates this backpressure to the sending peer's connection window, which in turn blocks the sending goroutine on the remote client.

This guarantees that a full disk causes the entire affected write path to stall rather than silently lose messages. The operator must resolve the disk condition; no messages are dropped.

### 5.4 Subscriber Offset Tracking

Each subscriber has a durable offset file recording the **global byte offset** of the next unread record in the channel's logical stream. The offset is written **after** the subscriber's handler returns successfully — this is the at-least-once contract.

A new subscriber starts at the global offset of the stream end (last segment's start offset + last segment's size), skipping pre-existing history. A restarting subscriber resumes from the persisted offset.

**Startup freshness filtering:** A subscription may set a maximum message age (`WithMaxAge`). On its first run such a subscriber fast-forwards its offset past any buffered messages older than the cutoff, so it begins delivery near real time instead of replaying a stale backlog after a restart or reconnect. The fast-forward skips whole sealed segments whose last write predates the cutoff without reading them — a record's timestamp is at most its write time, so an old-mtime sealed segment cannot contain a fresh record — then scans only the single boundary segment by envelope timestamp to land on the first fresh record. Filtering happens **only at startup**: once delivery begins no message is ever skipped for age, so a subscriber that falls behind at runtime still receives every message (preserving state-dependent processing). New subscribers, which already start at the stream end, are unaffected; this matters for resuming subscribers. Bytes skipped are reported via the `StartupSkippedBytes` subscriber stat.

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

**Transient failures (`ErrRetryLater`):** A handler can return `messenger.ErrRetryLater` (directly or wrapped) to signal that a failure is transient — e.g. a downstream sink is temporarily unavailable. Unlike an ordinary error, this does **not** count against `max_retries` and does **not** dead-letter the message. The subscriber stops processing the current batch, leaves the offset unadvanced, and re-attempts the same message on the next change notification or poll. The durable channel log buffers the backlog (subject to retention — see §5.7), so messages queue during an outage and delivery resumes from the same position once the downstream recovers. It is the inverse of dead-lettering: use it when dropping a message is worse than pausing delivery. Pauses are surfaced via the `RetryLaterPauses` subscriber stat.

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

**Segment rolling:** The writer rolls to a new segment when the current segment's size would exceed `compaction_threshold_mb` (default: 256 MB). Rolling is O(1): the current segment is synced and closed, a new file is created with a name encoding its start offset, and subsequent writes land in the new file. There is no pause, copy, or coordination with subscribers.

**Compaction:** The compactor periodically scans the channel directory and deletes any sealed segment (all segments except the active one) whose entire content lies before the minimum subscriber offset — i.e., every registered subscriber has advanced past the segment's last byte. Deletion is a single `unlink` syscall. No writer pause is needed: the writer holds the active segment open, and Unix permits deletion of sealed segments even while subscribers hold open file descriptors to them (the inode persists until all readers close their fds).

**Retention caps (force-eviction):** Consumption-based compaction alone lets a single parked or lagging subscriber pin a channel's log into unbounded growth, eventually filling the disk. Two optional bounds cap usage by force-deleting the oldest sealed segments **even if a subscriber has not consumed them**:

- `max_channel_size_mb` — a hard cap on a channel's total retained on-disk bytes.
- `retention` — a maximum message age. A sealed segment's file mtime is used as the age of its newest record (a record's timestamp is at most its write time). Accepts a day unit, e.g. `7d` or `168h`.

Eviction fires when **either** bound is exceeded; both default to `0` (disabled), preserving pure consumption-based behavior. Sealed segments are evaluated oldest-first and deleted while consumed, over the size cap, or past the age bound; the active segment is never deleted, so the effective floor is one segment (keep `compaction_threshold_mb` small to keep that floor low). Federation peer offsets participate in the minimum-offset boundary like any subscriber, so a lagging peer's unconsumed data is subject to the same force-eviction.

**Subscriber fast-forward on undercut:** When force-eviction drops data a subscriber had not yet read, that subscriber's persisted offset falls below the earliest surviving segment's start. On its next read the subscriber detects this, fast-forwards to that start (skipping the dropped messages), logs a warning, and continues — preventing a negative-seek wedge. The event is surfaced via the `CompactionDrops` subscriber stat.

### 5.8 Subscriber Registration

Subscribers must be explicitly registered before consuming. Registration writes an initial offset file (at the current end-of-file for new subscribers, or reads the existing offset for resuming subscribers). The compaction process uses the registered subscriber list to determine the safe deletion boundary.

Deregistering a subscriber removes its offset file and allows compaction to proceed past that subscriber's last position.

---

## 6. Federation

### 6.1 Instance Identity

Each instance is identified by a human-readable **instance name**. The config file has no identity field; the source depends on what the instance is doing:

- **Federation enabled** (`hub.enabled` or `client.enabled` is true): TLS is mandatory and the name is derived from the local TLS certificate's Common Name. The hub extracts the CN from each peer's certificate at connection time and uses it for all allowlist checks and audit log entries. `New()` returns an error if federation is enabled without TLS.
- **Local-only** (neither hub nor client enabled): TLS is optional. If TLS is configured, the cert CN is still used. Otherwise the name falls back to `os.Hostname()`.

Library tests that exercise non-TLS code paths may use the test-only `WithTestIdentity(name)` option to set a deterministic identity. Production code MUST NOT call it.

If multiple instances need to run on the same physical host they require separate certificates with distinct CNs (e.g. `billing-host` and `orders-host`). The CN is identity only; it is not used for network addressing. Because the federation TLS verifier checks the CA chain only (see §6.4), the cert's DNS SAN does not need to match the address callers use to reach the instance — operators are free to choose CNs that describe the instance's role rather than its hostname or port.

The instance name is used for:
- Allowlist authorization (hub verifies the cert CN against its `allowed_peers` config)
- The `origin` field in message envelopes
- Audit log entries
- Human-readable log messages

The message `id` field (UUID v7) is separate from the instance name. It is generated per-message and used exclusively for deduplication. It is not an instance identifier.

### 6.2 gRPC Connection

Federation uses gRPC over mutual TLS. Both sides present certificates; both sides verify the peer cert against the shared CA. The minimum TLS version is hardcoded to 1.3 and is not configurable — this is a closed federation mesh, and there is no operational reason to allow downgrade to older TLS versions.

The gRPC service exposes two bidirectional streaming RPCs:

```protobuf
service FederationService {
  rpc Publish(stream PublishBatch)   returns (stream PublishAck);
  rpc Subscribe(stream SubscribeFrame) returns (stream HubBatch);
}
```

**Publish RPC** — carries messages from client to hub:
- Client streams `PublishBatch` frames (one or more serialised envelopes per frame).
- Hub streams `PublishAck` frames back, one per received batch.
- The hub extracts the client's identity from the TLS peer certificate CN. For plain (test-only) connections, identity falls back to the `x-federation-instance` gRPC metadata key.

**Subscribe RPC** — carries messages from hub to client:
- The client sends a single `SubscribeRequest` as the first frame, declaring the channel list it wants to receive. Client identity is carried in the `x-federation-instance` gRPC metadata header (same as Publish) and validated against the TLS cert CN; it is not part of the request body.
- The hub then streams `HubBatch` frames containing envelope records.
- The client acknowledges each batch by sending an `Ack` frame back on the same stream.
- A `CloseNotice` frame from the hub signals an orderly shutdown.

A client that only publishes and does not want to receive any messages opens only the Publish stream. A client that only subscribes opens only the Subscribe stream. Full-duplex clients open both.

### 6.3 Message Wire Format

Each `PublishBatch` or `HubBatch` record list carries one or more serialised envelope records. Individual records use the same `envelope.Marshal` format written to `.jsonl` files on disk — a single JSON line per envelope. gRPC's own HTTP/2 framing handles record delimitation; no additional length-prefix is applied by the application layer.

Batching is applied when more than one record is available at the time a sender frames its next outbound RPC message. The sender does not wait for additional records; if the channel reader returns a single envelope it is sent immediately. Maximum batch size is configurable (default: 4 MiB).

The receiver sends an acknowledgment (`PublishAck` or `Ack`) after writing a batch to its local `.jsonl` file. The sender persists its per-channel byte offset to disk after each ack; on reconnect, the next batch begins at the saved offset. There is no in-memory unacked window — the offset file is the entire delivery state.

### 6.4 Authorization: Two Layers

**Layer 1 — mTLS:** The TLS handshake verifies the peer holds a certificate signed by the configured CA. A peer with no valid cert or a cert from an unknown CA is rejected at the TLS layer before any application code runs. Verification is **CA-chain only**: hostname / DNS-SAN matching is deliberately disabled (`tlsutil.BuildTLSConfig` sets `InsecureSkipVerify` and supplies a `VerifyPeerCertificate` callback that runs `x509.Certificate.Verify` against the configured CA pool but never calls `VerifyHostname`). This means a cert is valid regardless of which address it is presented at; identity is decoupled from network location and carried entirely by the cert CN (see §6.1).

**Layer 2 — Allowlist:** After the TLS handshake, the hub extracts the peer's instance name from the TLS peer certificate CN. If TLS is active but the peer presents no certificate, or the certificate has no CN, the connection is rejected immediately. The extracted name is checked against the hub's configured `allowed_peers` list. A peer with a valid cert but an unrecognized CN is rejected with gRPC status `PermissionDenied` and the connection is recorded in the audit log.

For plain (non-TLS) connections — used only in unit/integration tests — the hub falls back to the `x-federation-instance` gRPC metadata key for identity. Plain connections from peers not in the allowlist are rejected with the same `PermissionDenied` status.

Clients never perform allowlist checks — a client accepts any hub it is configured to dial (trust is established by the cert).

### 6.5 Subscription Model and Channel Policy

#### 6.5.1 Client Configuration

Each outbound hub connection in `client.hubs` declares both directions independently:

```yaml
client:
  enabled: true
  hubs:
    - addr: "hub.internal:7740"
      subscribe: ["orders", "payments"]   # channels we request from the hub
      publish:   ["billing.created"]      # channels we send to the hub
```

The `subscribe` list controls which channels the hub streams to this client. The `publish` list controls which channels this client is permitted to send to the hub. An empty list means "all channels" for that direction.

When a client connects, the hub computes the effective subscribe channel set as:

```
effective_subscribe = client.subscribe ∩ hub_subscribe_allowlist_for(client)
```

The hub creates a `clientCoordinator` with one `channelReader` per subscribed channel only when the effective subscribe set is non-empty. A client that sends no `subscribe` list (or an empty one) will not receive any messages from the hub.

There are no wildcards; all channel names are exact strings.

#### 6.5.2 Hub Access Control for Peers

Each entry in `allowed_peers` specifies what channels that peer is permitted to subscribe to (receive from hub) and publish to (send to hub):

```yaml
hub:
  allowed_peers:
    - name: "billing-host"
      subscribe: ["metrics", "alerts"]  # channels this peer may receive from hub
      publish:   ["billing.events"]     # channels this peer may send to hub
    - name: "monitor"
      subscribe: []                     # empty = permit any channel
      publish:   []                     # empty = permit any channel
```

An empty `subscribe` list means the peer may request any channel. An empty `publish` list means the peer may send messages on any channel. Non-empty lists act as allowlists; the hub silently restricts to the intersection.

The `publish` allowlist is an inbound filter: if a peer sends a message on a channel not in the list, the message is discarded and recorded in the audit log as a policy violation. This is a defense-in-depth measure against peer misconfiguration.

#### 6.5.3 General Notes

Forwarding is independent of whether any local subscriber currently exists for the channel. Messages are written to the local `.jsonl` file regardless of local subscriber presence.

### 6.6 Message Deduplication

Every instance (hub and client alike) maintains an in-memory LRU set of recently-seen message IDs. The set holds the last 100,000 IDs (configurable). TTL-based expiry is not used; the LRU eviction bound is sufficient for the expected message rates.

On receiving a message via federation (PeerReceiver):

1. Check the seen-ID set.
2. If present: discard silently.
3. If absent: add to set, write to local file, notify local subscribers.

On `Publish()` (local origin):

1. Add the message ID to the seen-ID set before writing to disk.
2. If the message later arrives back via a federation path (e.g., because a peer hub reflects it), step 2 above catches it.

The dedup set is shared across all PeerReceivers on the same Messenger instance. This ensures that if the same envelope arrives via two simultaneous incoming connections (dual-path forwarding), only the first arrival triggers a local write.

**Note on ring topologies:** Because outbound delivery is driven from the per-channel segment files (see §6.7), federated-received messages that are written to local storage by `writeLocalEnvelope` ARE eligible for re-forwarding when an outbound reader is configured for the same channel — a relay instance forwards them onward. The dedup LRU is the live loop-prevention mechanism: if a forwarded message returns to its origin via some ring topology, the origin's seen-ID set will reject the duplicate local write. Note that the re-send still consumes wire bandwidth before being rejected; for ring-prone topologies, restricting `publish` allowlists per peer is the right operational control.

Practical scenarios the dedup handles today:
- Two client connections from the same publisher both deliver the same envelope to a hub (dual-path)
- A publisher's own message arrives back via a federated receive path on a multi-hop topology (self-loop prevention)
- Reconnect replay delivering messages already written in a previous session

### 6.7 File-Reader Delivery and Reconnection

Both hub-side and client-side federation delivery use a unified **file-reader pull model**, mirroring the local subscriber delivery path. The two directions share the same `channelReader` machinery and differ only in the gRPC stream type they drive (`HubBatch` on the server-side Subscribe stream vs `PublishBatch` on the client-side Publish stream) and the offset-file prefix used to namespace their state.

| Direction | Coordinator | gRPC stream | Offset file prefix |
|---|---|---|---|
| Hub → peer (inbound peer subscription) | `clientCoordinator` | server-side Subscribe | `fed-{peerName}.offset` |
| Client → hub (outbound publish) | `pubCoordinator` | client-side Publish | `fedout-{hubAddr}.offset` |

Operation in both directions:

- For each `(peer-or-hub, channel)` pair, a `channelReader` goroutine watches the local segment files at `{dataDir}/channels/{channel}/`.
- When a message is written to a channel (via local `Publish` or `writeLocalEnvelope` from inbound federation), the messenger calls `NotifyChannel(channel)` on every registered notify target — the hub's notify registry for inbound peers, and each client's reader map for outbound channels.
- Each `channelReader` reads from the segment files starting at its current byte offset, accumulates envelopes up to the configured batch size, and delivers them to the coordinator as one `sendReq`.
- The coordinator serialises sends across all readers for one connection: one batch is in-flight at a time; the next batch is not sent until the peer acknowledges the current one.
- After the ack, the reader persists the new byte offset atomically to its offset file. The file is the entire delivery state — there is no in-memory unacked window.

**Offset files and compaction:** Both prefixes (`fed-` and `fedout-`) live under `subscribers/{channel}/` and are automatically included in the compactor's minimum-offset boundary calculation. A hub cannot compact past a peer that hasn't acked; a publishing client cannot compact past a hub that hasn't acked. On a colocated relay process (both hub and client roles), the two namespaces coexist without interference.

**Reconnect:** When a peer connection drops, its coordinator exits. The reconnect loop closes the active readers + coordinator and dials again with exponential backoff (base 500ms, max 60s, jitter ±20%). On the new connection, fresh `channelReader` instances resume from the saved offset files, so any messages published locally during the disconnect are delivered on reconnect. There is no in-memory queue to lose; bounded disconnects are bounded only by disk capacity.

**New reader starting position:** When a reader is created for the first time (no offset file exists), it starts at the current end of the channel. For inbound peer subscriptions this means the peer receives only messages published after it first connects (avoids replaying unbounded history). For outbound client publishing this means the client only forwards messages published from this point on — historic messages from before federation was configured are not back-filled.

**Offset file TTL:** On the hub side, peers that disconnect and never reconnect would otherwise block compaction indefinitely. The hub runs a background TTL sweep (configurable via `hub.fed_client_offset_ttl`, default 1 week) that deletes `fed-*.offset` files whose mtime is older than the TTL. The sweep matches only the `fed-` prefix, so client-side `fedout-*.offset` files on a colocated process are untouched.

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
  └─ fsync if sync_interval_ms = 0
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

The wire format between hubs uses the same JSON envelope bytes, carried as records within gRPC `PublishBatch` or `HubBatch` protobuf messages. No additional length-prefix is applied at the application layer; gRPC's HTTP/2 framing handles delimitation.

### 9.4 Concurrency Model

| Component | Goroutine model |
|---|---|
| Per-channel file writer | 1 goroutine per channel, owns the file descriptor |
| Per-subscriber reader | 1 goroutine per subscriber, independent cursor |
| Per-(peer,channel) federation reader | 1 goroutine per (connection, subscribed channel), reads segment files and feeds the coordinator |
| Per-connection coordinator + ack-reader | 2 goroutines per active federation connection, own all writes/reads on the gRPC stream |
| Per-peer Subscribe receiver | 1 goroutine per client→hub Subscribe stream, dispatches inbound envelopes to `localWriter` |
| Policy watcher | 1 goroutine, shared |
| Audit writer | 1 goroutine, shared |

No shared mutable state is accessed without synchronization. Per-channel and per-peer goroutines communicate via channels, not mutexes, except for the seen-ID LRU (protected by a read-write mutex).

---

## 10. Configuration

### 10.1 Loading Configuration

Configuration can be loaded from a YAML file or constructed programmatically in Go.

**From YAML file:**

```go
cfg, err := messenger.LoadConfig("/path/to/config.yaml")
if err != nil {
    log.Fatal(err)
}
m, err := messenger.New(cfg)
```

**Programmatically:**

```go
cfg := &messenger.Config{
    Name: "my-instance",
    Storage: messenger.StorageConfig{
        DataDir:               "/var/keyop",
        SyncIntervalMS:        200,
        OffsetFlushIntervalMS: 200,
    },
}
cfg.ApplyDefaults()  // Fill in unset fields with defaults
if err := cfg.Validate(); err != nil {
    log.Fatal(err)
}
m, err := messenger.New(cfg)
```

`ApplyDefaults()` fills in zero-valued fields with their documented defaults. `Validate()` checks that all required fields are set and values are in valid ranges.

### 10.2 Configuration Reference

```yaml
# Full configuration with all fields and defaults.
# Note: instance identity is not configurable. When federation is enabled,
# it is derived from the local TLS cert's CN (TLS is mandatory in that case).
# For local-only instances with no TLS, it falls back to the OS hostname.

storage:
  data_dir: "/var/keyop"   # Required. Root directory for channel files, offset files, audit log.
  sync_interval_ms: 0      # 0 = fsync after every write (default, strictest durability, slowest);
                           # > 0 = fsync periodically at interval in milliseconds (batched, faster).
  offset_flush_interval_ms: 0  # 0 = flush subscriber offset after every message (strictest at-least-once);
                           # > 0 = batch offset flushes (faster; may redeliver up to this window on crash).
  compaction_threshold_mb: 256 # Writer rolls to a new segment when the active segment reaches this size.
  max_channel_size_mb: 0   # 0 = unlimited; > 0 force-evicts oldest segments to cap a channel's retained bytes.
  retention: 0             # 0 = no age limit; e.g. "7d" / "168h" force-evicts segments older than this.
                           # max_channel_size_mb and retention drop the oldest data even if unconsumed.

subscribers:
  max_retries: 5           # Retry count before routing a message to the dead-letter channel.
                           # Omitting the field (nil) applies the default of 5.
                           # Setting it explicitly to 0 routes to dead-letter immediately on
                           # the first failure with no retries.

hub:
  enabled: false
  listen_addr: ""          # e.g. "0.0.0.0:7740"

  allowed_peers:           # Instance names permitted to connect to this hub
    - name: "billing-host"
      subscribe: []        # Channels this peer may receive from hub; empty = any channel
      publish: []          # Channels this peer may send to hub; empty = any channel
    - name: "orders-host"
      subscribe: ["metrics", "alerts"]
      publish: ["orders.created", "orders.updated"]

client:
  enabled: false
  hubs:                    # Hubs to dial (this instance as a client)
    - addr: ""             # host:port
      subscribe: []        # Channels to request from this hub (exact names, no wildcards)
      publish: []          # Channels to send to this hub (exact names, no wildcards)

tls:
  cert: ""                 # Path to instance certificate (PEM)
  key:  ""                 # Path to instance private key (PEM)
  ca:   ""                 # Path to CA certificate (PEM)
  expiry_warn_days: 30     # Log a warning this many days before cert expiry
                           # Minimum TLS version is hardcoded to 1.3.

federation:
  reconnect_base_ms: 500
  reconnect_max_ms: 60000
  reconnect_jitter: 0.2
  send_buffer_messages: 10000   # DEPRECATED: ignored by the regular Messenger client
                                # (the channel file is the buffer). Still honored by
                                # EphemeralMessenger.WriteQueueSize when set there.
  max_batch_bytes: 4194304      # Max gRPC message payload size (default 4 MiB)

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

### 11.2 Ephemeral Flag

The ephemeral client sets `ephemeral: true` in the `SubscribeRequest` proto message sent as the first frame on the Subscribe stream:

```protobuf
SubscribeRequest {
  version:   "1"
  subscribe: ["alerts"]
  ephemeral: true
}
```

Client identity is carried in the `x-federation-instance` gRPC metadata header and validated against the TLS cert CN, exactly as for the durable client.

If the client only publishes and opens no Subscribe stream, the flag is irrelevant — publish-only clients never receive messages regardless.

When the hub sees `ephemeral: true`, it does not create per-channel offset files for that peer — messages published before the connection are not delivered, and no offset file is written to disk. The hub logs ephemeral connections at a distinct info message so operators can distinguish them from durable client connections.

### 11.3 Publish Semantics

`EphemeralMessenger.Publish` (and the underlying `EphemeralClient.Publish`) blocks until one of:

| Outcome | Return value |
|---|---|
| Hub writes the message and sends a `PublishAck` | `nil` |
| Connection drops before the ack arrives | `ErrEphemeralConnLost` |
| `ctx` is cancelled or deadline exceeded | wrapped `ctx.Err()` |
| `Close()` was called | `ErrEphemeralClosed` |

On `ErrEphemeralConnLost`, the message **may or may not** have been received by the hub — the caller must decide whether to retry. There is no automatic retransmission; that is the caller's responsibility.

Multiple `Publish` calls may be batched into a single `PublishBatch` gRPC message when they arrive concurrently. The hub acks the entire batch with one `PublishAck`; all callers in the batch are unblocked simultaneously. The hub uses the standard `Publish` stream handler, so all deduplication, policy enforcement, and local storage semantics are identical to regular federation.

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

The outbound buffer (depth 256 by default) persists across reconnects. Items enqueued by `Publish` while disconnected wait until a new connection is established. If the buffer fills, the message is dropped with a warning logged; callers are not blocked (fire-and-forget behaviour for ephemeral publishers).

With `AutoReconnect: false`, `Connect` dials once. After disconnect, subsequent `Publish` calls return `ErrEphemeralConnLost` until `Connect` is called again.

### 11.6 Goroutine Model

Each connection opens up to two gRPC streams on a single shared `grpc.ClientConn`. The `ClientConn` is created once at construction time and reused across reconnects.

**Publish stream** (always opened):

| Goroutine | Role |
|---|---|
| `PeerSender.run` | Drains the outbound buffer, sends `PublishBatch` frames, blocks for `PublishAck` before sending the next batch; exits on stream error or `Close()` |

**Subscribe stream** (opened only when subscribe channels are configured):

| Goroutine | Role |
|---|---|
| `PeerReceiver.run` | Owns all reads on the Subscribe stream; dispatches `HubBatch` records to in-memory handlers; sends `Ack` frames after each batch |

With `AutoReconnect: true`, an additional reconnect goroutine runs for the lifetime of the client. On disconnect it waits for the current `PeerSender` to exit, captures unacknowledged messages from its buffer, dials a new pair of streams, and re-enqueues the unacknowledged messages onto the new sender.

All goroutines are tracked in `EphemeralClient.wg`; `Close()` cancels all stream contexts and calls `wg.Wait()`, ensuring clean shutdown with no dangling goroutines.

### 11.7 No Audit Log

`EphemeralClient` does not write an audit log. Inbound messages dispatched to handlers use a `noopAuditLogger`. The hub's own audit log records ephemeral client connections (`client_connected`) and disconnections (`peer_disconnected`) via its standard `serveConn` path.

### 11.8 Differences from Regular Client

| Aspect | Regular `Client` | `EphemeralClient` |
|---|---|---|
| Local storage | Writes received messages to `.jsonl` | None — in-memory handlers only |
| Replay on reconnect | Hub resumes from stored per-peer offset file | Never replays; `ephemeral: true` in `SubscribeRequest` |
| Publish ack | Unacknowledged messages are buffered and replayed on reconnect | Ack unblocks callers; on loss caller receives `ErrEphemeralConnLost` |
| Subscriber offset | Durable offset file per subscriber | No offset file |
| Audit log | Full hub events | No audit log on client side |
| Dedup | Shared instance-wide LRU (100k IDs) | Small per-connection LRU (1024 IDs, defense-in-depth) |
