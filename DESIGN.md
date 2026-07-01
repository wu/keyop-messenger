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

Relay chains and **cyclic** peerings (an instance that both subscribes to and publishes to the same channel, or peer hubs that forward to each other) are loop-safe: the per-message path vector (§6.6) stops a message from being forwarded back to any instance already in its path, so duplicates are never re-sent across the wire.

### 3.2 What Is Not Supported

- Direct client-to-client connections
- Automatic channel discovery or subscription propagation between hubs
- Wildcard channel patterns in forwarding policy (exact channel names only)
- **Multiple processes sharing a single `data_dir`.** Each data directory must be owned by exactly one running messenger process. The storage layer relies on a single in-process writer goroutine per channel and an in-process notifier to wake subscribers; it does not coordinate writers or wake subscribers across process boundaries. Running two processes against the same `data_dir` would interleave appends to the same segment files and leave cross-process subscribers dependent on the safety poll alone. To run multiple instances on one host, give each its own `data_dir` and connect them via federation.

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
  "route":        ["billing-host", "hub-west"],
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
| `route` | array of string | Path vector: the ordered set of instance identities (TLS cert CNs) that have durably committed this message — the original publisher followed by each forwarding hub/peer. Used for routing-loop prevention (see §6.6). Bounded by the network diameter. Omitted when empty. |
| `payload` | object | Arbitrary JSON object. Shape is defined by the payload type.                                                   |

The envelope is intentionally minimal. Infrastructure fields live in the envelope; all application semantics live in `payload`.

**Ordering note:** Messages within a single channel on a single instance are totally ordered by write sequence. Across federated hubs, messages from different origins arrive in network order, not origin-timestamp order. The `ts` field reflects when the message was published at its origin and can be used for approximate ordering, but clock skew between hosts means it is not a reliable global ordering key. No reorder buffer is provided; subscribers must tolerate out-of-order delivery across hub boundaries.

### 4.2 Payload Type Registry

Publishers register a payload type string and a corresponding Go type at startup:

```go
messenger.RegisterPayloadType("com.keyop.orders.OrderCreated", OrderCreated{})
```

Subscribers receive the decoded `payload` field as the registered type. A payload whose `payload_type` is not registered (or whose JSON does not decode into the registered type) is **not** delivered to the handler: the durable subscriber routes it to the channel's dead-letter queue (so it is recoverable, never silently lost), and the ephemeral subscriber — which has no dead-letter queue — skips it with a warning. Unregistered payloads are no longer coerced into `map[string]any`; that fallback silently lost any non-object payload (a JSON array, string, or number) because it could not unmarshal into a map.

### 4.3 Envelope Versioning

The `v` field allows future changes to the envelope schema. Readers must handle unknown `v` values gracefully (log and skip, or pass through raw). Version `1` is the only defined version.

**Additive fields within a version.** New optional fields may be added within an existing version without a bump, provided they are `omitempty` and absence decodes to a safe zero value. The `route` field was introduced this way: an envelope written by a build that predates it simply has no `route` key and decodes to an empty path vector, which loop prevention treats as "unknown provenance" and falls back to the dedup cache (see §6.6). A version bump is reserved for changes that would break decoding by an older reader.

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

The writer appends with `O_APPEND` and issues a single `write()` syscall per record. Because each `data_dir` is owned by a single process (§3.2) and every append to a channel is serialized through this one writer goroutine, no cross-writer locking is needed regardless of record size — there is never a second writer to interleave with.

What "write confirmed" means depends on `sync_interval_ms`:

| `sync_interval_ms` | `Publish()` returns after                                                    | Durable against                                            |
|--------------------|------------------------------------------------------------------------------|------------------------------------------------------------|
| `0`                | `fsync()` completes                                                          | OS crash, power failure                                    |
| `> 0`              | `write()` syscall returns; fsync runs on a background timer at that interval | Application restart; OS crash only between fsync intervals |

For `sync_interval_ms=0`, an fsync failure is returned as an error to the caller. For `sync_interval_ms>0`, fsync failures are logged as warnings and retried on the next timer tick; `Publish()` is not affected.

**Batched writes.** The writer also accepts a *batch* request (`WriteBatch`) carrying multiple records. The batch is a single rendezvous request handled atomically with respect to other writes: each record is appended in order through the same single-record write path, but **one `fsync` and one subscriber notification cover the whole batch** when `sync_interval_ms=0`. This amortises the per-record fsync — the dominant write cost — across the batch while keeping strict durability. The fsync is the cost being amortised; the per-record `write()` syscalls are not coalesced.

`WriteBatch` follows the same written/not-written contract as a single write: it returns `nil` only after the entire batch is durably committed (fsynced, when `sync_interval_ms=0`), and a non-nil error means the batch was **not** fully committed and may be safely retried. An empty batch is a no-op. Crucially, the batch's confirmation is never returned ahead of the commit — the same invariant that single writes uphold, extended to the group.

A batch never splits a record across a segment boundary. If appending the next record would exceed the segment size limit, the writer rolls to a new segment at the record boundary (fsyncing and sealing the current segment first, as for single writes), so a single batch may span multiple segments but every record lands wholly within one. Each roll necessarily fsyncs the sealed segment, so a batch large enough to roll performs one fsync per segment boundary plus the final fsync — the single-fsync amortisation applies to the common case where the batch fits the active segment.

On a mid-batch write failure the records already appended may remain on disk; because the caller treated the batch as not-acked and retries the whole batch, at-least-once is preserved — the retry may duplicate the record-aligned prefix that survived (a crash leaves only a trailing partial record, which recovery truncates; see §5.1). `WriteBatch` is the storage-layer primitive behind the public `PublishBatch(ctx, channel, msgs)` API — which builds one envelope per message (each `BatchMessage` carries its own payload type, so one batch may mix event types on a channel; the channel, correlation ID, and service name are shared), marks each in the dedup set, and commits them in one `WriteBatch` call — and, in a later phase, batched federation ingest. All consumers share its single-commit, single-notify behaviour.

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

When a subscriber's handler returns an error or panics, the message is retried up to a configurable maximum (`max_retries`, default: 5). Between retry attempts the subscriber observes an **exponential backoff** delay: the first retry waits `retry_backoff_base_ms` (default 100ms), doubling on each subsequent attempt and capping at `retry_backoff_max_ms` (default 5s). Together these three knobs set the total retry window before the message is dead-lettered. If `Stop()` is called during a backoff sleep the subscriber exits immediately without advancing the offset — the message will be re-delivered on the next start. On the final retry failure:

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

Dead-letter channels are regular channels in their storage layout. They can be subscribed to for monitoring or reprocessing. Dead-letter channels are not themselves subject to dead-lettering (a failing dead-letter handler is logged and the offset advanced).

**Reserved suffix.** The `.dead-letter` suffix is reserved for the messenger. `Publish` and `PublishBatch` reject any channel name ending in `.dead-letter` with `ErrReservedChannelName`, so a dead-letter channel only ever contains messenger-generated dead-letter envelopes and never application traffic that happens to collide with the naming convention. Subscribing to a dead-letter channel is still permitted — that is how monitoring and reprocessing consumers read it. The internal dead-letter write path does not go through `Publish`; the subscriber writes the dead-letter envelope directly to the channel's writer, so the reserved-suffix guard does not impede it. Because that write bypasses `Publish`, the subscriber's dead-letter writer is wrapped so each write still increments the dead-letter channel's message counter — without the wrapper a dead-letter channel's `MessageCount` (and the `DeadLetterMessages` total in §12) would stay at zero even as envelopes accumulate.

**Default retention.** An undrained dead-letter channel — one with no reprocessing consumer — would otherwise grow without bound, since consumption-based compaction never reclaims segments no subscriber has read. To bound this, dead-letter channels are given a default retention of **7 days** when no global `storage.retention` is configured: the compactor force-deletes sealed dead-letter segments older than that even though nothing consumed them (see §5.7). When a global `storage.retention` *is* configured, dead-letter channels inherit it like any other channel.

Age-based retention only acts on **sealed** segments — the active (currently-written) segment is never deleted, and a segment only seals when a write would push it past the channel's roll threshold. To keep that floor low for dead-letter channels, they roll at a separate, smaller threshold: `storage.dead_letter_compaction_threshold_mb` (default **50 MB**, versus `storage.compaction_threshold_mb`'s 100 MB for regular channels). A smaller roll size means a quiet dead-letter channel seals its active segment sooner, so the 7-day retention can actually reclaim it rather than holding everything in one never-rolled segment. Lower it further if you need a tighter bound on a low-volume channel.

**Head-of-line blocking.** Each subscriber runs its own delivery goroutine with its own offset, and retries — including the exponential-backoff sleeps between attempts — run inline in that goroutine. So a consistently-failing message blocks **that subscriber's** delivery of every later message on the channel until it is dead-lettered. The stall is per subscriber: other subscribers on the same channel read from their own offsets and keep delivering, unaffected. At the default `max_retries` of 5 that is roughly **3.1 s** (100 + 200 + 400 + 800 + 1600 ms) of stalled delivery per poison message for the affected subscriber; raising `max_retries` lengthens the stall (each further attempt adds up to the 5 s backoff cap). This is intentional backpressure — for an ordered stream of state-change events you generally *want* to stall rather than process message N+1 before N succeeds — so for many channels the blocking is a feature, not a problem.

**Sizing the retry window.** The right question is usually not "how do I avoid blocking" but "how long should a single message retry before giving up." The default window is only ~3.1s, which rides out a brief blip (a GC pause, a momentary network hiccup) but **not** a downstream restart, deploy, or DB failover (typically 10–60s). Two distinct tools cover the two regimes:

- **Bounded transient errors** (the same message might succeed on a quick retry — optimistic-concurrency conflict, a transient 500): widen the retry window via `retry_backoff_base_ms` / `retry_backoff_max_ms` / `max_retries`. For example `max_retries` ≈ 11 at the default schedule gives ~30s, enough to ride out a typical restart. The trade-off is a correspondingly longer head-of-line stall for that subscriber.
- **Indefinite downstream outages, and any channel where dropping a message is unacceptable**: prefer returning `ErrRetryLater` (see below) over a large retry budget. **Dead-lettering is itself the order-breaking event** — when a message finally dead-letters, the offset advances and message N+1 is delivered while N sits in the DLQ, leaving a permanent gap in an ordered state stream. `ErrRetryLater` never dead-letters: it pauses the subscriber at the same offset and re-attempts until the downstream recovers, preserving order indefinitely without consuming the retry budget.

`max_retries`, `retry_backoff_base_ms`, and `retry_backoff_max_ms` are instance-wide defaults, but the retry window is set **per subscriber**: a `Subscribe` call may pass `WithMaxRetries(n)` and `WithRetryBackoff(base, max)` to override them for that subscriber alone. Each subscriber has its own `Subscriber` with its own retry state, so two subscribers reading the **same** channel can choose different regimes — e.g. an ordered consumer that must not skip a message (wide window, or `ErrRetryLater`) and a latency-sensitive consumer that prefers to dead-letter quickly (short window). The policy is per subscriber, not per channel or per process.

**Transient failures (`ErrRetryLater`):** A handler can return `messenger.ErrRetryLater` (directly or wrapped) to signal that a failure is transient — e.g. a downstream sink is temporarily unavailable. Unlike an ordinary error, this does **not** count against `max_retries` and does **not** dead-letter the message. The subscriber stops processing the current batch, leaves the offset unadvanced, and re-attempts the same message on the next change notification or poll. The durable channel log buffers the backlog (subject to retention — see §5.7), so messages queue during an outage and delivery resumes from the same position once the downstream recovers. It is the inverse of dead-lettering: use it when dropping a message is worse than pausing delivery. Pauses are surfaced via the `RetryLaterPauses` subscriber stat.

### 5.6 File Change Notification

Because each `data_dir` is owned by a single process (see §3.2), subscribers and publishers always share an address space, and wake-ups are delivered in-process rather than through the filesystem.

**In-process notifier:** Subscribers are woken via a `LocalNotifier` — a capacity-1 `chan struct{}` that the writer goroutine signals directly after each successful write (or once per batch). On wake-up the subscriber reads from its current offset to EOF, so a single notification covers any number of records appended since the last read. The notifier never blocks the writer: it uses a non-blocking send, so multiple writes that arrive while the subscriber is mid-read coalesce into at most one pending wake-up.

**Safety poll:** The subscriber loop also wakes on a 1-second ticker. Because the notification channel has capacity 1, a burst of writes that occur while the subscriber is inside `processAvailable` leaves only one pending token; if the scanner happened to observe EOF just before the last write became visible, the periodic re-check guarantees the data is still delivered rather than waiting for the next write. `processAvailable` is idempotent — offset tracking skips already-delivered records — so the extra poll is cheap and never double-delivers. The poll is the upper bound on delivery latency in the (rare) lost-wake-up case; the notifier carries the common path.

### 5.7 File Rotation and Compaction

**Segment rolling:** The writer rolls to a new segment when the current segment's size would exceed `compaction_threshold_mb` (default: 100 MB). Rolling is O(1): the current segment is synced and closed, a new file is created with a name encoding its start offset, and subsequent writes land in the new file. There is no pause, copy, or coordination with subscribers.

**Compaction:** The compactor periodically scans the channel directory and deletes any sealed segment (all segments except the active one) whose entire content lies before the minimum subscriber offset — i.e., every registered subscriber has advanced past the segment's last byte. Deletion is a single `unlink` syscall. No writer pause is needed: the writer holds the active segment open, and Unix permits deletion of sealed segments even while subscribers hold open file descriptors to them (the inode persists until all readers close their fds).

**Retention caps (force-eviction):** Consumption-based compaction alone lets a single parked or lagging subscriber pin a channel's log into unbounded growth, eventually filling the disk. Two optional bounds cap usage by force-deleting the oldest sealed segments **even if a subscriber has not consumed them**:

- `max_files` — a cap on the total number of segment files a channel retains, **including the active segment**. The active segment is never deleted, so the on-disk ceiling is `max_files` files (and the floor is 1). **Defaults to `10`**, so disk usage is bounded out of the box; an explicit `0` disables the cap (unbounded, pure consumption-based behavior). The per-channel byte ceiling is therefore roughly `max_files × compaction_threshold_mb` (≈ 1 GB at the defaults of 10 × 100 MB) — keep `compaction_threshold_mb` small if you need a tighter bound.
- `retention` — a maximum message age. A sealed segment's file mtime is used as the age of its newest record (a record's timestamp is at most its write time). Accepts a day unit, e.g. `7d` or `168h`. Defaults to `0` (disabled).

Eviction fires when **either** bound is exceeded. Sealed segments are evaluated oldest-first and deleted while consumed, over the file count, or past the age bound; the active segment is never deleted, so the effective floor is one segment. Federation peer offsets participate in the minimum-offset boundary like any subscriber, so a lagging or disconnected peer's unconsumed data is subject to the same force-eviction.

**Reader fast-forward on undercut:** When force-eviction drops data a subscriber or federation peer had not yet read, its persisted offset falls below the earliest surviving segment's start. A durable subscriber detects this on its next read, fast-forwards to that start (skipping the dropped messages), logs a warning, and continues — preventing a negative-seek wedge; the event is surfaced via the `CompactionDrops` subscriber stat. The federation outbound `channelReader` does the same by construction: its read seeks to `max(storedOffset, earliestSegment.startOffset)`, so a client that stays disconnected long enough for the cap to drop unconsumed segments resumes, on reconnect, at the oldest message still available rather than replaying the lost range or stalling.

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

> **Footgun: never share a certificate between two running instances.** The CN *is* the identity, and loop prevention (§6.6.1) is keyed on it. Two processes that present the same certificate are, to the federation, the **same node** — and the path vector cannot tell them apart. Concretely, if a metrics-publisher process and a dashboard process on one host share a cert with CN `host-a`:
> 1. the publisher publishes a metric; its `route` is seeded with `host-a` and it is forwarded to the hub;
> 2. the dashboard subscribes to that channel from the same hub;
> 3. when the hub delivers the metric toward the dashboard, the loop guard sees `host-a` — the dashboard's own identity — already in `route`, concludes the message has looped back to a node that already holds it, and drops it (send-side at the hub, or receive-side at the dashboard).
>
> The result: **the dashboard instance never receives messages that the sibling process published**, even though both are healthy and correctly subscribed. This is by design — the guard is doing exactly its job of not re-delivering a message to an instance already in its path — but it is surprising because the two processes are logically distinct. The fix is to give each instance its own certificate with a unique CN. This is the same requirement as the distinct-CN rule above; the path vector just raises the stakes from "confused allowlist/audit" to "cross-delivery suppressed."
>
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
- The client declares the channels it publishes via the `x-federation-publish-channels` gRPC metadata header (one value per channel), set from its configured `publish` list. This is **advisory** — used by the hub for observability (audit detail and hub stats), not for enforcement. The header is optional: a client that omits it (e.g. an older build) is reported using the hub's `publish` allowlist instead. Because the stock client only builds outbound readers for its configured `publish` channels, the declared set always equals the set it actually forwards; the two cannot drift for a conforming client.

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

The `publish` allowlist is the **sole** authority for what the hub accepts. The client's `x-federation-publish-channels` declaration (see §6.2) is never used for enforcement: a message on a channel the client did not declare is still accepted if and only if the `publish` allowlist permits it. A non-conforming client could therefore publish an undeclared-but-allowlisted channel, in which case the declaration understates actual traffic; this is an observability inaccuracy, not an authorization gap. Enforcing the declaration (rejecting undeclared channels, or failing fast when the declared set and the allowlist are disjoint) is intentionally **not** done today.

#### 6.5.3 General Notes

Forwarding is independent of whether any local subscriber currently exists for the channel. Messages are written to the local `.jsonl` file regardless of local subscriber presence.

### 6.6 Deduplication and Loop Prevention

Two complementary mechanisms keep messages from being processed or forwarded more than once:

- **Path vector (§6.6.1)** — the primary loop guard. A durable, per-message record of the instances a message has visited, carried in the envelope's `route` field. It prevents a message from being forwarded back to any instance that already holds it, including around cyclic topologies, and is enforced **send-side** so an echo never crosses the wire. Because it travels with the message, it survives restarts and arbitrarily long disconnects.
- **LRU dedup (below)** — suppresses duplicate *arrivals* of the same message ID at a receiver. It handles the case the path vector does not: **diamond/fan-in**, where the same message reaches a node via two distinct, non-looping paths. It is soft, in-memory state and is no longer relied on as the primary loop guard.

Every instance (hub and client alike) maintains an in-memory LRU set of recently-seen message IDs. The set holds the last 100,000 IDs (configurable). TTL-based expiry is not used; the LRU eviction bound is sufficient for the expected message rates.

On receiving a batch via federation — the client's `PeerReceiver` or the hub's
Publish handler — a durable receiver **marks IDs before the write and rolls back
on failure**, so dual-path dedup stays exact while a failed commit loses nothing:

1. For each record, atomically mark it with `SeenOrAdd`. If it was already seen — a prior delivery, a concurrent dual-path arrival, or an intra-batch repeat — skip it.
2. Accumulate the survivors and commit them to local storage as one durable unit (a single `WriteBatch`; see §5.2) **before** acking.
3. On success: audit the forwards, notify local subscribers, and ack the batch. On failure: **un-mark** the accepted IDs (`dedup.Remove`), do **not** ack, and let the sender resend from its un-advanced offset (see §6.7). The un-mark is what lets the resent batch be re-accepted instead of dropped as a duplicate.

Marking *before* the write keeps loop/dual-path suppression atomic and exact: of two simultaneous arrivals of the same ID, the first `SeenOrAdd` wins and the second is suppressed. The only path that ever un-marks is one whose own commit failed — and that same path withholds its ack and resends, so the message is still delivered exactly once in the common case and at-least-once always. Ephemeral clients, which dispatch to in-memory handlers rather than a durable store, mark-and-write per record with the same `SeenOrAdd`.

On `Publish()` (local origin):

1. Add the message ID to the seen-ID set before writing to disk.
2. If the message later arrives back via a federation path (e.g., because a peer hub reflects it), step 2 above catches it.

The dedup set is shared across all PeerReceivers on the same Messenger instance. This ensures that if the same envelope arrives via two simultaneous incoming connections (dual-path forwarding), only the first arrival triggers a local write.

**Note on ring topologies:** Because outbound delivery is driven from the per-channel segment files (see §6.7), federated-received messages that are written to local storage by `writeLocalEnvelope` ARE eligible for re-forwarding when an outbound reader is configured for the same channel — a relay instance forwards them onward. Loops are prevented by the path vector (§6.6.1), which is checked send-side, so a message is not re-forwarded to an instance already in its `route` and the echo never leaves the sender. The dedup LRU is no longer the loop guard for this case; it remains the guard for fan-in (below).

Practical scenarios the dedup LRU handles today:
- Two client connections from the same publisher both deliver the same envelope to a hub (dual-path)
- The same message reaches one node via two distinct, non-looping forwarding paths (diamond fan-in) — the path vector does not suppress this because neither arrival is a loop
- Reconnect replay delivering messages already written in a previous session

#### 6.6.1 Loop Prevention (Path Vector)

The envelope's `route` field (§4.1) is an ordered **path vector**: the set of instance identities that hold a durable copy of the message. It is maintained by two rules that together guarantee one invariant.

**Invariant:** every envelope in an instance's local store contains that instance's identity in `route`.

Maintained by:
1. **On publish** (`Publish()` / local origin) — `route` is seeded with the publisher's own identity.
2. **On inbound commit** — before an instance durably writes a federated-received message, it appends its own identity to `route` (idempotent: appended only if absent). Since the local writer re-marshals the envelope struct, the appended identity is persisted in the segment file and travels on any onward forward.

An instance's identity is its TLS certificate CN (§6.1) — the same authenticated value used everywhere else, so `route` entries cannot be spoofed. Because loop prevention is keyed on this identity, two running instances must never share a certificate: the guard would treat them as one node and silently suppress cross-delivery between them through a shared hub. See the foot-gun callout in §6.1.

**Primary enforcement is send-side.** Each outbound `channelReader` (§6.7) knows the identity of the peer it feeds. Before forwarding a record it checks whether that destination already appears in the record's `route`; if so the record is an echo and is dropped **before it reaches the wire** (the offset still advances past it, so it is not rescanned). Because the check is per-destination, a message can be dropped for one peer while still being forwarded to another. This is what makes cyclic and mesh topologies loop-safe: a message circulating a ring stops at the first hop that would return it to an instance already in its path.

The destination identity comes from the TLS certificate on both sides, but is obtained differently:

- **Hub → subscriber:** the hub authenticates each peer from its certificate CN when the Subscribe stream is accepted (`authenticatePeer`). The destination identity is known synchronously; there is no gap.
- **Client → hub:** the client learns the hub's identity from the **verified server certificate at the TLS handshake**, captured by a credentials wrapper and stored on the client; the outbound readers read it lazily. Because gRPC dials lazily, on the **very first connect** the readers can begin evaluating records before the handshake has populated the hub's CN. During that sub-second, first-connect-only window the client's send-side check is skipped (unknown destination → do not filter), and the receive-side backstop catches any echo that slips through. The captured value persists for the life of the process, so reconnects reintroduce no gap.

**Receive-side backstop.** On inbound commit, before appending itself, an instance checks whether its **own** identity is already in `route`. If so the message has looped back to a node that already holds it, and it is dropped (audited as `loop_dropped`, §8). This backstop requires only the instance's own identity — never the peer's — so it always applies. It covers the windows send-side cannot:
- the sub-second first-connect gap on the client described above;
- **non-TLS (insecure) connections**, where the client cannot learn the hub's CN from a certificate and so does no send-side filtering (TLS is mandatory whenever federation identity matters, so this is a dev/test consideration);
- **pre-upgrade peers** during a rolling deployment — nodes running a build that predates `route`. They neither seed, append, nor check the path vector, so messages they originate or relay carry an empty or partial `route`. An upgraded node still self-protects via the backstop (it stamped its own identity on the way out) and, for genuinely empty routes, falls back to the dedup LRU — i.e. exactly the pre-change behavior, with no regression. Once every node is upgraded, all messages carry a complete route from birth and the send-side guarantee is fully effective.

**Path vector vs. dedup.** The path vector prevents *loops* — a message returning to a node it has already visited. It does **not** suppress *diamond fan-in*, where the same message reaches a node via two distinct paths that each never revisit a node; both arrivals pass the loop check and the LRU dedup collapses them by message ID. The two mechanisms are complementary.

**Observability.** This is a net improvement over the old dedup-only design, which dropped duplicates silently. Loop suppression is now surfaced: receive-side drops emit a `loop_dropped` audit event, and send-side drops (the common case) are logged at debug level by the outbound reader. Because send-side is where echoes are stopped in a healthy, fully-upgraded mesh, `loop_dropped` audit events should be rare — a sustained rate is itself a diagnostic signal (first-connect window, a pre-upgrade peer, or the shared-certificate foot-gun in §6.1). See §8 for the full description and how to read these signals.

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

**Receive-side batched commit:** On the receiving end — both the hub's Publish handler (client→hub) and a durable client's `PeerReceiver` (hub→client) — all accepted records of a wire batch are committed in a single `WriteBatch` (one fsync), and the ack is sent only after that commit returns (see §6.6). The ack is therefore never sent ahead of the durable write: the sender advances its offset solely on the ack, so a commit failure (no ack) leaves the sender's offset un-advanced and the whole batch is resent on the next connection. This amortises the per-record fsync across the batch while keeping the at-least-once guarantee strict — the receiver never tells the sender "done" for data it has not durably stored. Both directions share one `commitInboundBatch` implementation.

**Offset files and compaction:** Both prefixes (`fed-` and `fedout-`) live under `subscribers/{channel}/` and are automatically included in the compactor's minimum-offset boundary calculation. A hub cannot compact past a peer that hasn't acked; a publishing client cannot compact past a hub that hasn't acked. On a colocated relay process (both hub and client roles), the two namespaces coexist without interference.

**Reconnect:** When a peer connection drops, its coordinator exits. The reconnect loop closes the active readers + coordinator and dials again with exponential backoff (base 500ms, max 60s, jitter ±20%). On the new connection, fresh `channelReader` instances resume from the saved offset files, so any messages published locally during the disconnect are delivered on reconnect. There is no in-memory queue to lose; bounded disconnects are bounded only by disk capacity.

**New reader starting position:** When a reader is created for the first time (no offset file exists), it starts at the current end of the channel. For inbound peer subscriptions this means the peer receives only messages published after it first connects (avoids replaying unbounded history). For outbound client publishing this means the client only forwards messages published from this point on — historic messages from before federation was configured are not back-filled.

**Offset file TTL (inbound `fed-`):** On the hub side, peers that disconnect and never reconnect would otherwise block compaction indefinitely. The hub runs a background TTL sweep (configurable via `hub.fed_client_offset_ttl`, default 1 week) that deletes `fed-*.offset` files whose mtime is older than the TTL. The sweep matches only the `fed-` prefix.

**Outbound `fedout-` reconciliation:** Clients build one `fedout-{hubAddr}.offset` file per (hub, channel) and never delete them on `Close`, so a hub removed from the client configuration would otherwise leave its `fedout-*.offset` behind forever — and because the compactor includes `fedout-` files in its minimum-offset boundary, that orphan would permanently anchor compaction and leak disk. The hub `fed-` sweep does not cover this (wrong prefix, and it only runs when a hub is enabled). Instead, at startup `New()` reconciles the `fedout-` namespace against the configured hub set: any `fedout-*.offset` whose hub address is not in `client.hubs` is deleted (and when the client role is disabled, all of them are). Reconciliation runs at startup because clients are built at `New()`, so a configuration change only takes effect on restart. Matching is done in sanitized-filename space; inbound `fed-` files and ordinary subscriber offsets are never touched. This is config-driven, not mtime-driven: a configured-but-permanently-unreachable hub still anchors compaction until it is removed from the config (unlike the inbound `fed-` TTL sweep). On a colocated relay process the two namespaces are reaped by their respective mechanisms without interference.

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
| `loop_dropped` | Inbound message dropped by the receive-side loop guard: this instance was already in the message's `route` (see §6.6.1) |
| `policy_violation` | Inbound message rejected by receive policy |
| `replay_gap` | Reconnecting peer's last-ack ID was compacted away; delivery continues from earliest available |
| `peer_connected` | A peer hub connection was established |
| `peer_disconnected` | A peer hub connection was lost |
| `client_connected` | A client connected to this hub |
| `client_rejected` | A client was rejected (name not in allowlist) |

**Loop-drop visibility.** Before the path vector (§6.6.1), a message dropped as a duplicate was silently swallowed by the dedup LRU — there was no record of it. Loop suppression is now observable:

- **`loop_dropped`** audit events record every message dropped by the **receive-side** guard (this instance was already in the `route`). A steady stream of them is a signal, not noise — in a healthy, fully-upgraded mesh, echoes are stopped send-side (below) and `loop_dropped` should be rare, so a sustained rate points at a real condition: the first-connect handshake window, a pre-upgrade peer, or a misconfiguration.
- **Send-side** drops — the common case, where an echo is discarded before it leaves the forwarder — are logged at **debug** level by the outbound reader (`dropping echo (dest already in route)`, with the channel, destination identity, and message ID). They are not audited, because the outbound reader has no audit logger.

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
  └─ signal waiting subscribers via the in-process LocalNotifier (see §5.6)
```

`Publish()` always blocks until the write is confirmed. There is no in-memory queue and no separate timeout or drop path — if the disk is unavailable, the publisher waits indefinitely (see §5.3 for backpressure propagation to federated senders).

A **batch** request (`WriteBatch`) follows the same path with the loop body repeated per record and the fsync + subscriber-notify hoisted out of the loop:

```
writer goroutine (batch request)
  └─ for each record in batch:
       └─ roll to a new segment first if it would exceed the size limit (§5.2)
       └─ single write() syscall  (retry on I/O error until success, §5.3)
  └─ fsync once if sync_interval_ms = 0          ← amortised across the batch
  └─ signal caller: whole batch confirmed
  └─ signal waiting subscribers once             ← one notification per batch
```

See §5.2 for the batch durability contract and segment-boundary handling.

### 9.2 Read Path

```
in-process LocalNotifier signal (or 1s safety-poll tick)
  └─ subscriber goroutine wakes
  └─ list segment files in channel directory (sorted by start offset)
  └─ for each segment starting at or after subscriber's global offset:
       └─ open segment, seek to (globalOffset - segmentStartOffset)
       └─ scan lines to EOF; dispatch each with retry + backoff
       └─ advance to next segment's start offset when current segment EOF reached
  └─ write global offset file after each successful dispatch or dead-letter
       → on write failure: log error, leave in-memory offset unchanged
```

Subscribers are woken by the in-process `LocalNotifier` (see §5.6), backstopped by a 1-second safety poll. The per-line scanner buffer is capped at `maxLineSize` (10 MiB). A record larger than this cannot be returned by the scanner, so it is skipped rather than wedging delivery: an error is logged, the offset is advanced past it (its end located by reading to the record's terminating newline), and the `OversizedSkipped` subscriber stat is incremented. An oversized record not yet terminated by a newline is treated as an in-flight write and left in place until it completes, so a message mid-flight is never skipped.

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
  compaction_threshold_mb: 100 # Writer rolls to a new segment when the active segment reaches this size.
  max_files: 10            # Max total segment files per channel incl. the active one (default 10). 0 = unlimited.
  retention: 0             # 0 = no age limit; e.g. "7d" / "168h" force-evicts segments older than this.
                           # max_files and retention drop the oldest data even if unconsumed.

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

## 12. Observability

`Messenger.Stats()` returns a point-in-time `Stats` snapshot of runtime metrics. Reads are best-effort and the snapshot is not atomic across channels — a counter may advance between the moment one channel is read and the next. It is the intended source for shipping metrics to external systems (Graphite, Prometheus, etc.).

### 12.1 Snapshot Structure

A `Stats` value has three parts:

- **`Channels []ChannelStats`** — one entry per known channel, dead-letter channels included. Each carries `StreamBytes` (monotonic total bytes ever appended — does not drop on compaction), `DiskBytes` (current on-disk footprint of surviving segments — shrinks on compaction), `MessageCount` (messages published since process start; resets on restart), and `Subscribers []SubscriberStats`. Each subscriber reports `LagBytes` (unread bytes between its offset and the stream end) and `OldestPendingUnixMs` (publish timestamp of the record it is parked on, or 0 when caught up — callers compute age as `now − OldestPendingUnixMs` at display time so it stays correct between polls and does not bake in clock skew).
- **`Totals`** — instance-wide aggregates, described in §12.2.
- **`Latency`** — per-stage latency aggregates, described in §12.3.
- **`Federation FederationStats`** — outbound `Clients` (per configured hub: `Connected`, `ReconnectCount`, `UnackedBytes`, `PublishRTT` — the client→hub transit latency aggregate — and `PublishSendFailures`, both see §12.3) and, when this instance runs a hub listener, an inbound `Hub` (`RecordsReceived`, `BatchesReceived`, `ConnectionsAccepted`/`ConnectionsRejected`, active `PublishConns`/`SubscribeConns`, `SubscribeRTT` — the hub→peer delivery latency aggregate — `SubscribeSendFailures` (both see §12.3), and per-`Peer` detail).

### 12.2 Totals and Golden Signals

`Stats.Totals` pre-sums the per-channel and per-subscriber data so a metrics exporter does not have to walk the `Channels` tree itself. Dead-letter channels are excluded from the non-dead-letter aggregates and reported separately, keeping the error signal clean. Each total is computed in the same `Stats()` pass that builds `Channels`, at no extra I/O cost, so it always equals the sum of the snapshot it accompanies. The fields cover **three of the four** golden signals — Traffic, Saturation, and Errors. The fourth, **Latency**, is reported separately in `Stats.Latency`; see §12.3.

The table below classifies each total on two independent axes. **Golden signal** is which of the four SRE signals the field serves (`—` for fields that are operational inventory rather than a golden signal). **Type** is how to graph it: a *counter* is start-relative and resets to zero on restart (graph as a rate, e.g. Graphite `nonNegativeDerivative`, which absorbs the reset), whereas a *gauge* is an instantaneous level read fresh each snapshot.

| Field | Golden signal | Type | Meaning |
|---|---|---|---|
| `MessagesPublished` | Traffic | counter | Sum of `MessageCount` over non-dead-letter channels |
| `StreamBytes` | Traffic | counter | Sum of `StreamBytes` over non-dead-letter channels (byte throughput; survives compaction) |
| `DiskBytes` | Saturation | gauge | Sum of `DiskBytes` over non-dead-letter channels — current live-data footprint |
| `LagBytes` | Saturation | gauge | Sum of every subscriber's `LagBytes` — total unread backlog; the primary "consumers falling behind" signal |
| `DeadLetterMessages` | Errors | counter | Sum of `MessageCount` over dead-letter channels |
| `DeadLetterBytes` | Errors | gauge | Sum of `DiskBytes` over dead-letter channels |
| `Channels` | — | gauge | Count of non-dead-letter channels (inventory) |
| `Subscribers` | — | gauge | Count of active subscribers across non-dead-letter channels (inventory) |

Two notes on the classification:

- There is deliberately **no Latency total** in `Totals`. The per-subscriber `OldestPendingUnixMs` is sometimes mistaken for latency, but it measures **backlog age** — how stale the single oldest *un*consumed message is — which is a saturation-adjacent indicator, not the latency golden signal. True latency (how long a publish takes to reach disk, how long consumers take to process a message, how long a message takes to move between a client and a hub) is a distribution over *delivered* operations, not a max/min over pending timestamps, so it lives in its own `Stats.Latency` field rather than `Totals`. See §12.3.
- `DeadLetterMessages` (a counter) depends on the dead-letter write counter described in §5.5 and so resets on restart, whereas `DeadLetterBytes` (a gauge) is read straight from disk and is therefore durable across restarts — the two error totals recover differently after a bounce.

### 12.3 Latency

Latency is the fourth golden signal. It is not a single number but a set of per-stage distributions, each a `LatencyStage` with two complementary views:

- **Mean, via cumulative `Count` + `SumNanos`** (summed nanoseconds). These are start-relative counters; an exporter derives the mean over any interval as `Δ(SumNanos)/Δ(Count)`, the same count/sum style as `Totals`, graphed as deltas (e.g. Graphite `nonNegativeDerivative`).
- **Recent percentiles `P50Nanos`/`P90Nanos`/`P99Nanos`**, computed server-side over a trailing **~60-second sliding window** so they reflect current behaviour rather than the whole process lifetime. They are self-contained (no diffing needed) and are `0` when the window has no samples. `WindowCount` reports how many samples fell in that window — the exact population the percentiles are computed from, so a consumer can show "N samples → pXX" coherently rather than pairing the percentiles with the much larger lifetime `Count`.

The percentiles come from an internal fixed-bucket histogram (`internal/latencyhist`): a 1-2.5-5-per-decade bucket scale from 10µs to 25s, with linear interpolation within the containing bucket and saturation at 25s for anything beyond. The bucket boundaries are deliberately **not** part of the public API — callers see only the three percentile numbers, so the contract carries no bucket assumptions. The window is a **fixed 6-slot ring** of 10-second slots (≈1 KB per stage, allocated once), so the effective window is 50–60s. There is no timer goroutine: a stale slot is zeroed and reused on the next *write* that maps to it, and reads simply exclude any slot older than the window. Memory is therefore constant — it does not grow even if `Stats()` is never called (a continuously-published, never-read window just overwrites its 6 slots as the ring wraps). Accuracy is bucket-limited (adequate for golden-signal monitoring, matching the Prometheus-histogram approach); the estimator is encapsulated behind `latencyhist.Window` and can be swapped (e.g. for a DDSketch) without changing the `Stats` API.

The three instance-wide stages live in `Stats.Latency`:

- **`PublishToDisk`** — time from a `Publish`/`PublishBatch` call (or a federation-received write) to the message being durably written (fsync), on the writing host's clock. One sample per write *operation*: a `PublishBatch` of N messages is a single sample carrying the whole batch's commit time, since the batch shares one fsync. Accumulated instance-wide at the write sites, not derived from the per-channel snapshot.
- **`Consume`** — end-to-end delivery age of *successfully consumed* messages: the time from publish (the envelope timestamp) to a subscriber's handler returning success. It spans the publisher and consumer clocks, so across federated hosts it carries clock skew; skew-induced negatives are clamped to zero. Aggregated across every subscriber (dead-letter monitors included — a DLQ reader is still a consumer); because percentiles can't be averaged, each subscriber keeps its own window histogram and `Stats()` **merges the buckets** across subscribers before computing the instance-wide percentiles.
- **`Handler`** — time spent inside subscriber handlers for successfully consumed messages, on the consumer's clock. Unlike `Consume` it excludes queue wait, isolating handler cost from delivery delay; `Consume ≥ Handler` for the same message.

Samples are recorded **only on successful delivery**, so failed and retried handler attempts do not skew the averages — a message that is ultimately dead-lettered contributes nothing to `Consume`/`Handler`. The backlog-age view (`OldestPendingUnixMs`, §12.1) is complementary: it covers *un*consumed messages, which these delivered-message aggregates by definition cannot.

Two further stages cover **federation transit**, one per direction. Both are reported on the federation snapshot rather than instance-wide, both are `LatencyStage` values, and both are measured as a stream send→ack round-trip on the *sender's* own clock — so they are free of cross-host clock skew, but they measure send→ack, which includes the receiver's own write (transit *plus* remote durability, not pure network time). In each direction the sender's coordinator keeps only one batch in flight, so each RTT is unambiguously its batch's, and a sample is recorded only on a successful ack:

- **`Federation.Clients[].PublishRTT`** (outbound, per hub connection) — the round-trip from a federation client sending a `PublishBatch` to receiving the hub's `PublishAck`. Accumulated on the `Client` so it survives reconnects. Covers the regular federation `Client`; the ephemeral client is not instrumented.
- **`Federation.Hub.SubscribeRTT`** (inbound, hub-wide) — the round-trip from the hub sending a batch to a subscribing peer over the Subscribe stream to receiving that peer's ack. Accumulated on the `Hub` across all subscribing peers. This is the inbound counterpart to `PublishRTT`: together they cover both directions a message can move between a client and a hub.

Both transit stages are `LatencyStage` values, so they carry the same recent p50/p90/p99 (per hub connection for `PublishRTT`, hub-wide for `SubscribeRTT`) in addition to the cumulative mean.

Because the RTT aggregates record **only successfully-acked batches**, each transit direction also carries an error counter for the batches the RTT excludes — an in-flight batch lost when the stream breaks: `Federation.Clients[].PublishSendFailures` and `Federation.Hub.SubscribeSendFailures`. These are plain counters (not `LatencyStage`), the Errors signal for federation transit. The serial send loop fails at most one batch per disconnect, and a deliberate shutdown is not counted, so the counter measures genuine delivery disruption rather than idle reconnects — distinct from `ReconnectCount` (which also ticks on idle reconnects) and `UnackedBytes` (a backlog gauge). A batch that fails here is retried from the persisted offset after reconnect, so a single message can contribute one failure and, later, one successful RTT sample.
