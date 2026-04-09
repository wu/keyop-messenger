# keyop-messenger — Implementation Plan

This document describes the phased implementation plan for the `keyop-messenger` Go library. Each phase has concrete deliverables, a test strategy, and an explicit statement of which earlier phases it depends on.

---

## 1. Module and Package Structure

### 1.1 Go Module Name

```
github.com/wu/keyop-messenger
```

### 1.2 Directory Layout

```
keyop-messenger/
├── go.mod
├── go.sum
│
├── messenger.go            # Top-level public API (Messenger type, New, Close, Publish, Subscribe)
├── config.go               # Config struct, YAML loading, defaults, validation
├── options.go              # Functional options for Messenger construction
│
├── internal/
│   ├── envelope/
│   │   ├── envelope.go     # Envelope struct, marshal/unmarshal, schema version constants
│   │   └── envelope_test.go
│   │
│   ├── registry/
│   │   ├── registry.go     # PayloadRegistry interface + defaultRegistry implementation
│   │   └── registry_test.go
│   │
│   ├── storage/
│   │   ├── writer.go       # Per-channel writer goroutine, sync policies, backpressure
│   │   ├── writer_test.go
│   │   ├── subscriber.go   # Subscriber goroutine, offset tracking, dead-letter routing
│   │   ├── subscriber_test.go
│   │   ├── offset.go       # Offset file read/write/fsync
│   │   ├── offset_test.go
│   │   ├── watcher.go      # fsnotify wrapper with polling fallback
│   │   ├── watcher_test.go
│   │   ├── compaction.go   # File rotation/compaction logic
│   │   └── compaction_test.go
│   │
│   ├── dedup/
│   │   ├── dedup.go        # LRU seen-ID set, thread-safe
│   │   └── dedup_test.go
│   │
│   ├── audit/
│   │   ├── audit.go        # Audit writer goroutine, rotation, event types
│   │   └── audit_test.go
│   │
│   ├── tlsutil/
│   │   ├── tlsutil.go      # TLS config builder, cert hot-reload, expiry warning
│   │   ├── keygen.go       # P-384 key+cert generation (CA and instance)
│   │   └── tlsutil_test.go
│   │
│   ├── testutil/
│   │   └── fakes.go        # Shared fake implementations of Logger, ChannelWriter,
│   │                       # AuditLogger, ChannelWatcher for use across test packages
│   │
│   └── federation/
│       ├── handshake.go    # Application-level handshake structs and framing
│       ├── framing.go      # Length-prefix binary frame encoding/decoding
│       ├── hub.go          # Hub: listen, accept clients, accept peer hubs
│       ├── hub_test.go
│       ├── peersender.go   # Per-peer sender goroutine (queue, batch, ack, replay)
│       ├── peerreceiver.go # Per-peer receiver goroutine (dedup, local write, policy check)
│       ├── client.go       # Client: dial hubs, reconnect backoff
│       ├── client_test.go
│       ├── policy.go       # ForwardPolicy, allowlist, atomic swap
│       ├── policy_test.go
│       └── policywatcher.go # fsnotify config watcher, hot-reload orchestration
│
└── cmd/
    └── keyop-messenger/
        └── main.go         # CLI entrypoint (cobra root + keygen subcommands)
```

### 1.3 Public vs Internal

Everything under `internal/` is unexported to callers of the library. The root package (`github.com/wu/keyop-messenger`) is the sole public surface. The `cmd/` subtree is a standalone binary, not part of the library API.

---

## 2. External Dependencies

| Dependency | Role | Rationale |
|---|---|---|
| `github.com/gorilla/websocket` v1.5.x | WebSocket transport | Proven, widely used; supports binary frames and custom TLS config cleanly |
| `github.com/fsnotify/fsnotify` v1.7.x | File change notification | Standard Go file-watch library; cross-platform (inotify/kqueue/FSEvents) |
| `github.com/google/uuid` v1.6.x | UUID v4 generation | Zero external C dependencies; correct random-based UUID generation |
| `gopkg.in/yaml.v3` v3.0.x | Config file parsing | Supports struct tags; round-trips cleanly |
| `encoding/json` (stdlib) | Envelope and audit serialization | Sufficient for correctness; `sync.Pool`-backed buffers handle allocation. No additional JSON library was added — the stdlib overhead is acceptable given the file-I/O-bound write path. |
| `github.com/hashicorp/golang-lru/v2` v2.0.x | LRU seen-ID set | Typed, fixed-capacity LRU with atomic `ContainsOrAdd`; well-maintained |
| `github.com/spf13/cobra` v1.10.x | CLI framework | Standard for multi-command CLIs in Go |
| `github.com/stretchr/testify` v1.11.x | Test assertions | `require` + `assert` reduce test boilerplate significantly |

No CGO dependencies are introduced. The module targets Go 1.22+.

---

## 3. Key Interfaces Defined Early

These interfaces are established before implementation begins so that later phases can code to them independently and tests can use fakes.

```go
// Logger is the structured logging interface injected by callers.
// Signature matches slog.Logger for drop-in compatibility.
type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
}

// PayloadRegistry maps type discriminator strings to Go types.
type PayloadRegistry interface {
    Register(typeStr string, prototype any) error
    Decode(typeStr string, raw json.RawMessage) (any, error)
    KnownTypes() []string
}

// ChannelWriter appends a pre-marshaled envelope to a channel file.
// Blocks until confirmed (rendezvous — no in-memory queue).
type ChannelWriter interface {
    Write(env *envelope.Envelope) error
    Close() error
}

// ChannelWatcher abstracts fsnotify vs polling so subscriber code is testable.
type ChannelWatcher interface {
    Watch(path string) (<-chan struct{}, error)
    Close() error
}

// AuditLogger writes structured audit events.
type AuditLogger interface {
    Log(event audit.Event) error
    Close() error
}

// Deduplicator is the seen-ID set.
type Deduplicator interface {
    SeenOrAdd(id string) bool // returns true if already seen
}

// PeerSender enqueues a message for forwarding to a peer hub.
type PeerSender interface {
    Enqueue(env *envelope.Envelope) bool // false if buffer full
    Close() error
}
```

---

## 4. Implementation Phases

---

### Phase 1 — Module Scaffold and Configuration

**Depends on:** nothing

**Deliverables:**

- `go.mod` with module path, Go version, and all external dependencies pre-declared.
- `go.sum` populated via `go mod tidy`.
- `config.go`: `Config` struct with all fields from the YAML reference in DESIGN.md. `Load(path string) (*Config, error)` reads YAML, applies defaults, validates. `Validate() error`. `ApplyDefaults()`.

  Defaults: `sync_policy="periodic"`, `sync_interval_ms=200`, `max_retries=5`, `reconnect_base_ms=500`, `reconnect_max_ms=60000`, `reconnect_jitter=0.2`, `send_buffer_messages=10000`, `max_batch_bytes=65536`, `seen_id_lru_size=100000`, `audit.max_size_mb=100`, `audit.max_files=10`, `compaction_threshold_mb=256`, `max_subscriber_lag_mb=512`, `tls.expiry_warn_days=30`.

- `options.go`: `Option` functional option type; `WithLogger`, `WithConfig`, `WithDataDir` constructors.

**Test strategy:**

- Table-driven `Validate()` tests: missing `data_dir`, invalid `sync_policy`, negative `max_retries`, zero `sync_interval_ms` with `periodic` policy, `hub.enabled=true` with empty `listen_addr`, `tls.cert` set without `tls.key`.
- Round-trip: marshal `Config` → YAML → `Load` → assert all fields equal. Verifies `ApplyDefaults` does not clobber explicitly-set values.
- File-not-found returns a wrapped error distinguishable via `errors.Is`.

---

### Phase 2 — Envelope and Payload Registry

**Depends on:** Phase 1

**Deliverables:**

- `internal/envelope/envelope.go`: `Envelope` struct with JSON tags (`v`, `id`, `ts`, `channel`, `origin`, `payload_type`, `payload`). `Marshal(env Envelope) ([]byte, error)` using `sync.Pool`-backed `bytes.Buffer`. `Unmarshal(data []byte) (Envelope, error)` with version check: unknown `v` returns `ErrUnknownVersion` (callers log and continue — never drop). `NewEnvelope(channel, origin, payloadType string, payload any) (Envelope, error)` generates UUID v4, sets `ts` to `time.Now().UTC()`, marshals `payload` into `json.RawMessage`.
- `DeadLetterPayload` struct: `Original Envelope`, `Retries int`, `LastError string`, `FailedAt time.Time`.
- `internal/registry/registry.go`: `defaultRegistry` implementing `PayloadRegistry`. `Register` uses `reflect.TypeOf(prototype)` to store the type; rejects duplicates with `ErrPayloadTypeAlreadyRegistered`. `Decode` JSON-round-trips into the registered type; if unregistered, returns `map[string]any` and logs a warning (never drops). `KnownTypes` returns sorted slice.

**Test strategy:**

- `Marshal`/`Unmarshal` round-trip for a complete envelope.
- Unknown `v` returns `ErrUnknownVersion`; caller can still access raw bytes.
- `NewEnvelope` produces a valid UUID v4 `id` and RFC3339Nano `ts`.
- Registry `Register`/`Decode` with a concrete struct; verify field values after decode.
- Registry `Decode` with unregistered type returns `map[string]any` (not error).
- Duplicate registration returns `ErrPayloadTypeAlreadyRegistered`.
- `sync.Pool` buffer test: call `Marshal` 10,000 times concurrently with `-race`.

---

### Phase 3 — Storage: Writer Goroutine

**Depends on:** Phase 2

**Deliverables:**

- `internal/storage/writer.go`: `channelWriter` implementing `ChannelWriter`. `NewChannelWriter(channelPath string, policy SyncPolicy, syncInterval time.Duration, notifyFn func()) (*channelWriter, error)` opens the channel file with `O_APPEND|O_CREATE|O_WRONLY`.

  Internal goroutine loop: receive `writeRequest{data []byte, done chan<- error}` via an **unbuffered** channel (rendezvous); call `os.File.Write`; on error, retry with 10ms sleep (indefinitely — backpressure); on success, call `notifyFn()`, signal `done`. For `sync_policy=always`, `fsync` before signaling `done`. For `sync_policy=periodic`, a background ticker calls `fsync` at the configured interval.

  `flock` advisory lock for records exceeding `PIPE_BUF` (4096-byte conservative floor). `Close()` drains the goroutine and closes the file.

- `SyncPolicy` type: `SyncPolicyNone`, `SyncPolicyPeriodic`, `SyncPolicyAlways`.

**Test strategy:**

- Functional: write 1000 envelopes sequentially; read back, assert line count and each line parses as valid JSON.
- Concurrent with `-race`: 50 goroutines each calling `Write` simultaneously; verify no torn lines.
- `SyncPolicyAlways`: mock `os.File` to assert `Sync()` called once per write.
- `SyncPolicyPeriodic`: assert `Sync()` called on timer tick, not per write.
- Backpressure: fake file returns `ENOSPC` for first N writes then succeeds; assert `Write` eventually returns and record is in the file.
- `Close()`: goroutine exits cleanly; subsequent `Write` calls return an error.

---

### Phase 4 — Storage: Offset Tracking and File Watcher

**Depends on:** Phase 3

**Deliverables:**

- `internal/storage/offset.go`: `ReadOffset(path string) (int64, error)`. `WriteOffset(path string, offset int64) error` — writes to a `.tmp` file, fsyncs, renames (atomic). `OffsetFileExists(path string) bool`.
- `internal/storage/watcher.go`: `NewChannelWatcher(logger Logger) (ChannelWatcher, error)` wraps `fsnotify`. `Watch(path string) (<-chan struct{}, error)` — registers path; returns a coalesced `chan struct{}` (multiple rapid FS events collapse to one send via non-blocking send). Fallback: if fsnotify fails to watch a path, log warning and start a 100ms polling goroutine writing to the same channel. `Close()` removes all watches.
- Same-process fast path: `LocalNotifier` — a `chan struct{}` passed as `notifyFn` from the channel writer directly to a subscriber's select, bypassing the FS watcher.

**Test strategy:**

- `WriteOffset`/`ReadOffset` round-trip at 0, max int64, and a mid-file value.
- Atomic write: simulate crash between write and rename using a fake filesystem; verify original offset file unchanged.
- Watcher integration: create temp file, start watcher, append a line, assert notification arrives within 500ms.
- Coalescing: write 100 lines rapidly; assert notification channel has far fewer than 100 tokens.
- Polling fallback: fsnotify returns a watch error; verify polling emits notifications.

---

### Phase 5 — Storage: Subscriber Goroutine and Dead-Letter

**Depends on:** Phases 3, 4

**Deliverables:**

- `internal/storage/subscriber.go`: `Subscriber` struct. `NewSubscriber(id, channelPath, offsetDir string, reg PayloadRegistry, maxRetries int, dlWriter ChannelWriter, watcher ChannelWatcher, logger Logger) (*Subscriber, error)`. On construction: if offset file missing, write current EOF offset (new subscriber); otherwise read existing offset (resuming subscriber).

  Internal goroutine: wait on watcher or `LocalNotifier`; on wake, seek to current offset, read to EOF with `bufio.Scanner`; for each line: `envelope.Unmarshal` → `registry.Decode` → call `HandlerFunc`; retry up to `maxRetries` on error or panic (recovered via `recover()`); on final failure: build `DeadLetterPayload`, publish to `{channel}.dead-letter` via `dlWriter`, advance offset, log error. After each successful dispatch or dead-letter routing, call `WriteOffset`.

  Dead-letter channels are not themselves dead-lettered: detect via `strings.HasSuffix(channel, ".dead-letter")`; on handler failure, log and advance offset only.

**Test strategy:**

- Happy path: write 3 envelopes to temp file, start subscriber, assert handler called 3 times with correct payloads.
- At-least-once: handler succeeds; stop and restart subscriber; assert handler is not called again (offset persisted).
- Retry: handler fails `maxRetries-1` times then succeeds; assert called exactly `maxRetries` times, offset advanced.
- Dead-letter: handler always fails; assert `DeadLetterPayload` published to dlWriter; offset advanced; delivery continues with next message.
- Panic recovery: handler panics; assert goroutine continues; dead-letter triggered.
- Dead-letter-channel subscriber: handler always fails; assert no dead-letter published, offset advanced, error logged.

---

### Phase 6 — Storage: Compaction

**Depends on:** Phases 3, 4, 5

**Deliverables:**

- `internal/storage/compaction.go`: `Compactor` struct. `RegisterSubscriber(id string)`, `DeregisterSubscriber(id string)` (removes offset file). `MinOffset() int64` returns minimum offset across all registered subscribers. `MaybeCompact(channelPath string, threshold int64, writer ChannelWriter) error`: if `MinOffset() > threshold`, pause the writer goroutine (signal it to hold), rewrite file from `MinOffset()` to EOF into a temp file, rename over original, adjust all offsets by subtracting `MinOffset()`, resume writer. Lag warning: if any subscriber offset is more than `max_subscriber_lag_mb` behind EOF, log warning.

**Test strategy:**

- Trigger: file with 10 MB of data, subscriber at offset 9 MB; assert file shrinks and offset adjusts.
- No compaction when min offset below threshold: file unchanged.
- Writer pause: no `Write` calls succeed while compaction holds the pause; they succeed immediately after.
- Multi-subscriber: min offset is correctly the minimum across all.
- Deregister: subscriber removed; compaction advances to higher offset.
- Lag warning: subscriber at offset 0 in a 600 MB file; assert warning logged.

---

### Phase 7 — Deduplication

**Depends on:** Phase 1 *(independent of Phases 2–6; can be developed in parallel)*

**Deliverables:**

- `internal/dedup/dedup.go`: `LRUDedup` implementing `Deduplicator`. Wraps `github.com/hashicorp/golang-lru/v2` with size `seen_id_lru_size`. `SeenOrAdd(id string) bool` uses the cache's atomic `ContainsOrAdd` method to avoid TOCTOU. `NewLRUDedup(size int) (*LRUDedup, error)`.

**Test strategy:**

- First call with new ID returns `false`; second call returns `true`.
- LRU eviction: add `size+1` unique IDs; first ID evicted; `SeenOrAdd` returns `false` again.
- Race test with `-race`: 100 goroutines each adding 1000 IDs; no data race.
- Benchmark `SeenOrAdd` at size 100,000 to establish latency baseline.

---

### Phase 8 — Audit Log

**Depends on:** Phase 1 *(independent of Phases 2–7; can be developed in parallel)*

**Deliverables:**

- `internal/audit/audit.go`: `Event` struct: `Ts time.Time`, `Event string`, `MessageID string`, `Channel string`, `Direction string`, `Peer string`, `PeerAddr string`, `Detail string` (all optional except `Event` and `Ts`). Event name constants: `EventForward`, `EventPolicyViolation`, `EventReplayGap`, `EventPeerConnected`, `EventPeerDisconnected`, `EventClientConnected`, `EventClientRejected`, `EventClientDrain`, `EventPolicyReloaded`, `EventPolicyReloadFailed`.

  `AuditWriter` implementing `AuditLogger`. `NewAuditWriter(dir string, maxSizeMB, maxFiles int, logger Logger) (*AuditWriter, error)`. Internal goroutine: receive `Event` from a buffered channel (capacity 1000 to absorb bursts); marshal to JSON line; write to `audit.jsonl`; check file size; rotate if needed. Rotation: rename `audit.jsonl` → `audit.jsonl.1`, shift `.1`→`.2` etc., delete files beyond `max_files`, open new `audit.jsonl`. If channel full: drop event and log to stderr — audit must not block message delivery. `Close()` drains channel, closes file.

**Test strategy:**

- Write 5 events, close writer, read file, assert 5 valid JSON lines with correct fields.
- Rotation: set `max_size_mb=1`, write until rotation triggers; assert `audit.jsonl.1` exists and all events accounted for.
- `max_files=3`: write enough for 4 rotated files; assert `.4` is deleted.
- Concurrent `Log`: 50 goroutines, 100 events each; no data race.
- `Close` test: goroutine exits and file closed cleanly.

---

### Phase 9 — TLS Utilities and Certificate Generation

**Depends on:** Phase 1 *(independent of Phases 2–8; can be developed in parallel)*

**Deliverables:**

- `internal/tlsutil/keygen.go`: `GenerateCA(validityDays int) (certPEM, keyPEM []byte, err error)` — P-384 EC key, self-signed CA cert. `GenerateInstance(caCertPEM, caKeyPEM []byte, name string, validityDays int) (certPEM, keyPEM []byte, err error)` — P-384 instance key; cert with `CN=name` and `DNS SAN=name`; signed by CA. PEM output.
- `internal/tlsutil/tlsutil.go`: `BuildTLSConfig(certFile, keyFile, caFile string, logger Logger) (*tls.Config, error)` — `MinVersion: tls.VersionTLS13`, `ClientAuth: tls.RequireAndVerifyClientCert`. `ExtractCN(cert *x509.Certificate) string`. `CheckExpiry(cert *x509.Certificate, warnDays int, logger Logger)`. `HotReloadTLSConfig(certFile, keyFile, caFile string, watcher ChannelWatcher, logger Logger) (*HotReloadTLS, error)` — watches files via `ChannelWatcher`; on change rebuilds and atomically swaps via `sync/atomic.Pointer`; `tls.Config.GetConfigForClient` reads current pointer so new connections use the new cert.

**Test strategy:**

- `GenerateCA`: parse PEM, assert `IsCA=true`, key is P-384.
- `GenerateInstance`: assert `CN=name`, DNS SAN contains name, cert verifies against CA.
- `BuildTLSConfig` with valid files: `MinVersion=TLS13`, non-nil `ClientCAs`.
- `BuildTLSConfig` with missing files: wrapped error.
- `CheckExpiry`: cert with `NotAfter=now+15 days`; assert warning logged.
- `HotReloadTLS`: write initial cert, overwrite file, assert `GetConfigForClient` returns new cert within 1 second.

---

### Phase 10 — Federation: Wire Framing and Handshake

**Depends on:** Phase 2

**Deliverables:**

- `internal/federation/framing.go`: `WriteFrame(w io.Writer, records [][]byte) error` — writes `[4-byte big-endian length][record bytes]` for each record into one buffer, sends as a single binary WebSocket message. `ReadFrame(r io.Reader) ([][]byte, error)` — reads one frame, splits on length prefixes. Returns `ErrFrameTooLarge` if a record exceeds `max_batch_bytes`.
- `internal/federation/handshake.go`: `HandshakeMsg{InstanceName, Role, Version}`. `SendHandshake`, `ReceiveHandshake` (text frames, separate from binary data frames). `AckMsg{LastID string}`. `SendAck`, `ReceiveAck`.

**Test strategy:**

- `WriteFrame`/`ReadFrame` round-trip with 0, 1, and 100 records of varying sizes.
- Record larger than max returns `ErrFrameTooLarge`.
- Empty frame (0 records) round-trips cleanly.
- Handshake round-trip over a `net.Pipe`-backed connection pair.
- Simulated `io.ErrUnexpectedEOF` mid-frame: error returned to caller.

---

### Phase 11 — Federation: Policy Engine

**Depends on:** Phases 8, 10

**Deliverables:**

- `internal/federation/policy.go`: `ForwardPolicy{Forward, Receive []string}`. `AtomicPolicy` wraps `sync/atomic.Pointer[ForwardPolicy]` for lock-free reads. `AllowForward(channel string) bool`, `AllowReceive(channel string) bool` — linear scan (channel lists are small). `PeerHubConfig{Addr, Forward, Receive}`. `AllowedClient{Name}`. `HubConfig{AllowedClients, PeerHubs}`. `IsClientAllowed(name string) bool`.
- `internal/federation/policywatcher.go`: `PolicyWatcher`. `NewPolicyWatcher(configPath string, hub *Hub, audit AuditLogger, logger Logger) (*PolicyWatcher, error)`. Watches config file via fsnotify. On change: parse `HubConfig` from YAML, validate (non-empty addrs, no duplicate peer names), call `hub.ApplyPolicy(newConfig)`. `ApplyPolicy` atomically swaps channel lists on existing connections; dials newly added peers; initiates drain-then-close for removed peers and removed allowlist clients. Logs `EventPolicyReloaded` or `EventPolicyReloadFailed`.

**Test strategy:**

- `AllowForward`/`AllowReceive`: exact match returns `true`; non-match returns `false`; empty list returns `false`.
- `AtomicPolicy` swap: readers calling `AllowForward` concurrently while writer swaps; no data race.
- `PolicyWatcher` integration: write config, start watcher, modify file, assert `ApplyPolicy` called within 1 second.
- Invalid config (missing addr): reload aborted, `EventPolicyReloadFailed` logged, existing policy unchanged.
- Removed peer: drain-then-close initiated (mock peer connection).

---

### Phase 12 — Federation: Hub, Client, and Peer Goroutines

**Depends on:** Phases 7, 8, 9, 10, 11

**Deliverables:**

- `internal/federation/hub.go`: `Hub`. `NewHub(cfg HubConfig, tlsCfg *tls.Config, localWriter func(*envelope.Envelope) error, dedup Deduplicator, audit AuditLogger, logger Logger) (*Hub, error)`. `Listen(addr string) error` — TLS-wrapped `net.Listener`; accepts connections; upgrades to WebSocket; exchanges handshake; extracts CN from peer cert; checks against allowlist. On rejection: close frame 4403, log `EventClientRejected`. On acceptance: start `peerReceiver` goroutine.
- `internal/federation/peersender.go`: `PeerSender` goroutine. Buffered channel of `*envelope.Envelope` (`send_buffer_messages` capacity). `Enqueue` — non-blocking; drops with warning if full. Internal goroutine: batch up to `max_batch_bytes` or until channel empty; `WriteFrame`; wait for ack; advance last-acked ID. On write error: trigger reconnect. Replay: on reconnect, re-send all unacked buffered messages. Exponential backoff with jitter.
- `internal/federation/peerreceiver.go`: `PeerReceiver` goroutine. Reads binary frames; for each record: `dedup.SeenOrAdd(id)` → skip if seen; `localWriter(env)` (blocks on disk full — backpressure propagates to TCP); check `AtomicPolicy.AllowReceive(channel)` → log `EventPolicyViolation` and discard if violated (still acked); log `EventForward` for each accepted message; send ack after each batch.
- `internal/federation/client.go`: `Client`. `Dial(hubAddr string, tlsCfg *tls.Config, ...) error` — connects, exchanges handshake, starts sender+receiver goroutines. On disconnect: reconnect with backoff; on reconnect, send last-acked ID so hub can replay.
- `Hub.ReplayFrom(lastID string, channels []string) <-chan *envelope.Envelope` — scans channel files forward from record matching `lastID`, emitting envelopes on matching channels. If `lastID` not found (compacted): log `EventReplayGap`, start from earliest available.
- `Hub.ApplyPolicy(newCfg HubConfig)` — atomic swap of channel lists; dial new peers; drain-then-close removed peers and removed allowlist clients.

**Test strategy:**

- Hub/client integration over `net.Pipe`: hub listens, client dials, sends 10 messages, assert `localWriter` called 10 times.
- mTLS: client with wrong CA cert rejected at TLS layer.
- Allowlist: valid cert but unknown name receives close 4403; `EventClientRejected` logged.
- Deduplication: same message ID sent twice; `localWriter` called once.
- Policy violation: peer sends on channel not in `receive`; discarded; `EventPolicyViolation` logged.
- Backpressure: `localWriter` blocks 200ms; receiver does not read further frames during that time.
- Reconnect replay: disconnect mid-stream, reconnect, assert buffered messages replayed exactly once.
- `ReplayFrom` with compacted-away lastID: `EventReplayGap` logged, replay from earliest.
- Batching: 200 small messages batched into fewer than 200 frames.
- Send buffer full: `send_buffer_messages` exceeded; warning logged, excess messages dropped.

---

### Phase 13 — Root Messenger API

**Depends on:** Phases 5, 6, 7, 8, 12

**Deliverables:**

- `messenger.go`: `Messenger` struct. `New(cfg *Config, opts ...Option) (*Messenger, error)` — constructs all internal components; validates data directory; creates directory structure (`channels/`, `subscribers/`, audit file). On construction: calls `CheckExpiry` on instance cert and CA cert.
- `Publish(ctx context.Context, channel, payloadType string, payload any) error` — looks up or creates `ChannelWriter`; calls `envelope.NewEnvelope`; adds to dedup set; calls `writer.Write(env)`; notifies same-process subscribers via `LocalNotifier`; enqueues to peer senders whose forward policy includes the channel.
- `Subscribe(ctx context.Context, channel, subscriberID string, handler HandlerFunc) error` — registers subscriber, creates offset file if needed, starts subscriber goroutine.
- `Unsubscribe(channel, subscriberID string) error` — stops goroutine, removes offset file.
- `RegisterPayloadType(typeStr string, prototype any) error` — delegates to registry.
- `Close() error` — graceful shutdown: stop new publishes; drain subscribers; close peer connections (drain-then-close); close audit writer; close channel writers. Idempotent.
- `config.go`: `EnsureDirectories(cfg *Config) error`.
- Channel name validation: `ValidateChannelName(name string) error` — `[a-zA-Z0-9._-]+`, non-empty, ≤255 bytes.

**Test strategy:**

- End-to-end: `New` → `RegisterPayloadType` → `Subscribe` → `Publish` → assert handler called with correctly typed payload within 500ms.
- At-least-once: stop `Messenger`, restart with same data dir, re-subscribe same ID, publish nothing; assert no phantom deliveries.
- Multiple subscribers on same channel: each receives all messages independently.
- Federation end-to-end: two `Messenger` instances with hub/client config; publisher on one, subscriber on other; assert cross-instance delivery.
- `Close` is idempotent: call twice, assert no panic.
- Invalid channel name: `Publish` returns `ErrInvalidChannelName`.
- `ctx` cancellation: cancelled context passed to `Subscribe`; goroutine exits within 1 second.

---

### Phase 14 — CLI: keygen Subcommands

**Depends on:** Phase 9

**Deliverables:**

- `cmd/keyop-messenger/main.go`: cobra root command. Subcommands under `keygen`:
  - `keygen ca [--out-cert ca.crt] [--out-key ca.key] [--days 730]` — calls `tlsutil.GenerateCA`, writes PEM files.
  - `keygen instance --ca ca.crt --ca-key ca.key --name billing-host [--out-cert billing-host.crt] [--out-key billing-host.key] [--days 730]` — calls `tlsutil.GenerateInstance`, writes PEM files.
- Both commands: refuse to overwrite without `--force`. Print cert subject, validity period, and SHA-256 fingerprint on success.
- `version` subcommand: prints module version from `debug.ReadBuildInfo`.

**Test strategy:**

- `keygen ca` in temp dir: assert both files written, parse PEM, verify `IsCA=true`, key is P-384.
- `keygen instance` with CA files: assert cert parses, `CN=--name`, chain verifies against CA.
- Refuse overwrite: create target file, run without `--force`, assert non-zero exit and file unchanged.
- `--force`: file is overwritten.
- Missing required flag `--ca`: assert usage error.

---

### Phase 15 — Hardening, Benchmarks, and Integration Tests ✓

**Depends on:** Phases 13, 14

**Deliverables:**

- `messenger_bench_test.go`: benchmark `Publish` throughput (single channel, no federation), `Subscribe` read latency (time from `Publish` return to handler invocation), federation round-trip latency.
- `integration_test.go` (build tag `//go:build integration`): hub + 2 clients; dedup via dual-path forwarding; policy hot-reload mid-stream; disk-full backpressure; compaction during active subscribers.
- `Makefile`: targets `build`, `test`, `test-integration`, `bench`, `lint`.
- `.golangci.yml`: enable `revive`, `govet`, `staticcheck`, `gosec`, `errcheck`, `exhaustive`.
- `CLAUDE.md` update: build/run/test commands, architecture overview, environment variables (`KEYOP_MESSENGER_DATA_DIR`, `KEYOP_MESSENGER_CONFIG`).

**Test strategy:**

- All unit tests pass: `go test -race ./...`
- Integration tests pass: `go test -race -tags integration -timeout 60s ./...`
- Benchmarks run without failure: `go test -run='^$' -bench=. -benchmem -benchtime=3s ./...`
- `golangci-lint run` and `go vet ./...` produce zero warnings.

**Implementation notes:**

- The dedup integration test (`TestIntegrationRingDedup`) uses a dual-client topology rather than a Hub1→Hub2→Hub1 ring, because `writeLocalEnvelope` does not re-enqueue received messages for outbound forwarding. Two `ClientHubRef` entries with the same address create two simultaneous connections from one Messenger to another; both enqueue the same envelope, and the shared dedup LRU on the receiving hub accepts only the first.
- JSON numbers in `map[string]any` payloads are decoded as `float64` by the standard library, not `int`. Tests that assert on specific integer payload values must cast via `float64`.

---

### Phase 16 — Ephemeral Client ✓

**Depends on:** Phase 12 (federation hub, PeerReceiver), Phase 13 (root Messenger API)

**Deliverables:**

- `internal/federation/handshake.go` (modified): added `Ephemeral bool \`json:"ephemeral,omitempty"\`` field to `HandshakeMsg`. The `omitempty` tag ensures the field is absent from non-ephemeral connections and hub responses, preserving wire compatibility with pre-Phase 16 hubs.

- `internal/federation/hub.go` (modified): in `serveConn`, the hub now checks `hs.Ephemeral` before starting replay (`ReplayFrom`). Replay is skipped entirely for ephemeral connections even if `LastID` is set. Ephemeral connections are logged at a distinct info message.

- `internal/federation/ephemeral_client.go` (new): `EphemeralClientConfig`, `EphemeralClient`, `ephemeralWriteItem`. Key methods: `NewEphemeralClient`, `AddHandler`, `Connect`, `ConnectWithReconnect`, `Publish`, `Close`. Internal: `startConn`, `dispatchEnvelope`, `writeLoop`, `reconnectLoop`. See DESIGN.md §11 for semantics. `noopAuditLogger` satisfies `audit.AuditLogger` by discarding all events.

  Goroutine design: three goroutines per connection (PeerReceiver, watcher, write loop) plus one optional reconnect loop. All tracked in `EphemeralClient.wg`; `Close` calls `wg.Wait()`. The watcher goroutine ensures `conn.Close()` is called before `wg.Wait()` returns, preventing goroutine leaks if the write loop is blocked.

  Per-message ack: `Publish` enqueues an `ephemeralWriteItem` with a buffered `doneCh` (cap 1). The write loop batches items from `writeQ`, sends one binary frame, then blocks on `ackCh` for the hub's text-frame ack. All items in the batch are signalled together via non-blocking sends to `doneCh`.

- `ephemeral.go` (new, root package): `EphemeralConfig`, `EphemeralMessenger`. Public methods: `NewEphemeralMessenger`, `RegisterPayloadType`, `Subscribe(channel string, handler func(Message))`, `Connect(ctx)`, `Publish(ctx, channel, payloadType, payload)`, `Close`. Re-exports `ErrEphemeralConnLost`.

- `ephemeral_test.go` (new, `//go:build integration`): 7 integration tests covering: publish-to-hub delivery, subscribe-from-hub delivery, no-replay on connect, context-cancelled publish, registered payload type decoding, auto-reconnect lifecycle, and concurrent multi-publish.

**Test strategy:**

- `TestEphemeralMessenger_Publish_ToHub`: publish via ephemeral client; assert hub subscriber receives the message.
- `TestEphemeralMessenger_Subscribe_FromHub`: hub publishes; assert ephemeral handler fires.
- `TestEphemeralMessenger_Subscribe_NoReplay`: publish before ephemeral connects; assert pre-connect message is not delivered; post-connect messages are.
- `TestEphemeralMessenger_Publish_ContextCancelled`: pre-cancelled context; assert `context.Canceled` returned immediately.
- `TestEphemeralMessenger_RegisterPayloadType`: typed payload decoded correctly in handler.
- `TestEphemeralMessenger_AutoReconnect`: connect, publish, close hub, close ephemeral; verify no deadlock.
- `TestEphemeralMessenger_MultiplePublish`: 20 concurrent publishes all succeed and are acked.

All tests use `hubLocalAddr(t, hub)` (returns `localhost:PORT`) and hub cert CN `"localhost"` so TLS hostname verification passes without IP SANs.

---

## 5. Public API Surface

```go
package messenger

type Config struct { ... }  // see config.go

func LoadConfig(path string) (*Config, error)

type Option func(*Messenger)

func WithLogger(l Logger) Option
func WithConfig(cfg *Config) Option

func New(cfg *Config, opts ...Option) (*Messenger, error)

type Messenger struct { ... }

func (m *Messenger) RegisterPayloadType(typeStr string, prototype any) error
func (m *Messenger) Publish(ctx context.Context, channel, payloadType string, payload any) error
func (m *Messenger) Subscribe(ctx context.Context, channel, subscriberID string, handler HandlerFunc) error
func (m *Messenger) Unsubscribe(channel, subscriberID string) error
func (m *Messenger) Close() error

type HandlerFunc func(ctx context.Context, msg Message) error

type Message struct {
    ID          string
    Channel     string
    Origin      string    // instance name of original publisher
    PayloadType string
    Payload     any       // decoded Go struct, or map[string]any for unregistered types
    Timestamp   time.Time
}

type Logger interface {
    Debug(msg string, args ...any)
    Info(msg string, args ...any)
    Warn(msg string, args ...any)
    Error(msg string, args ...any)
}

var (
    ErrPayloadTypeAlreadyRegistered = errors.New("payload type already registered")
    ErrInvalidChannelName           = errors.New("invalid channel name")
    ErrMessengerClosed              = errors.New("messenger is closed")
    ErrEphemeralConnLost            = errors.New("ephemeral client: connection lost before ack")
)

// EphemeralMessenger — stateless hub client (Phase 16)

type EphemeralConfig struct {
    HubAddr         string
    InstanceName    string
    Subscribe       []string
    TLS             TLSConfig
    AutoReconnect   bool
    ReconnectBase   time.Duration
    ReconnectMax    time.Duration
    ReconnectJitter float64
    MaxBatchBytes   int
    WriteQueueSize  int
}

func NewEphemeralMessenger(cfg EphemeralConfig, opts ...Option) (*EphemeralMessenger, error)

type EphemeralMessenger struct { ... }

func (m *EphemeralMessenger) RegisterPayloadType(typeStr string, prototype any) error
func (m *EphemeralMessenger) Subscribe(channel string, handler func(msg Message)) error
func (m *EphemeralMessenger) Connect(ctx context.Context) error
func (m *EphemeralMessenger) Publish(ctx context.Context, channel, payloadType string, payload any) error
func (m *EphemeralMessenger) Close() error
```

---

## 6. Testing Conventions

- All tests use `testify/require` (fatal) and `testify/assert` (non-fatal).
- All tests run with `-race` in CI. No test is exempt.
- Temporary files and directories use `t.TempDir()` (auto-cleaned).
- Fake implementations of `Logger`, `ChannelWriter`, `AuditLogger`, and `ChannelWatcher` live in `internal/testutil/fakes.go` so they can be imported across test packages without import cycles.
- Integration tests use build tag `//go:build integration` and are excluded from the default `go test ./...` run.
- Benchmarks live alongside unit tests but require `-bench` to run; they do not gate CI.
