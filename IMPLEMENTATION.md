# keyop-messenger — Federation File-Reader Delivery

This document describes the implementation plan for replacing the hub→subscriber push delivery model with a file-reader pull model, giving federation subscribers the same at-least-once delivery guarantees and compaction integration as local subscribers.

---

## Background and Motivation

### Current (broken) model

When a peer connects to the hub and subscribes to channels, the hub delivers messages by:

1. Calling `hub.EnqueueToAll(&env)` from `Publish` and `writeLocalEnvelope`, pushing in-memory envelopes into each peer's `PeerSender` buffer.
2. On reconnect, the client sends a `LastID` field in the handshake; the hub calls `ReplayFrom` which does a sequential scan of all channel segment files to find that ID and replay from there.

This has several fundamental problems:

- **No persistent delivery state on the hub.** If the hub restarts, it has no record of what each subscriber received. The only recovery mechanism is `LastID` sent by the client, which is itself broken: `ConnectWithReconnect` sends `sender.LastAckedID()`, which tracks the *outbound* (client→hub) direction, not the *inbound* (hub→client) direction. For subscribe-only clients this value is always empty, so the hub replays everything from the beginning on every restart.
- **`ReplayFrom` is incorrect for multiple channels.** Segments are collected in channel order and scanned sequentially. Any channel whose segments appear after the channel containing `lastID` is fully replayed regardless of whether it was already delivered.
- **No compaction integration.** The compactor only knows about local subscribers (via `subscribers/{channel}/` offset files). Federation subscribers are invisible to it, so the hub cannot safely determine when segments are safe to delete.

### New model

The hub reads from segment files on behalf of each connected subscriber, exactly as local subscribers do. Each subscribed channel gets its own `channelReader` goroutine that tracks a byte offset in the segment files and writes that offset to `subscribers/{channel}/fed-{peerName}.offset` after each acked batch. The compactor already reads all files in `subscribers/{channel}/`, so federation subscribers are automatically included in compaction boundary calculations.

All incoming connections — whether `Role: "client"` or `Role: "hub"` — use the same delivery mechanism. There is no distinction.

---

## What Changes

### Removed

| Item | Reason |
|---|---|
| `Hub.EnqueueToAll` | Replaced by `Hub.NotifyChannel` + file readers |
| `Hub.ReplayFrom` | No longer needed; reconnect resumes from stored offset |
| `hub.channelSubscribers` reverse index | Was only used by `EnqueueToAll` |
| `PeerSender` on hub side (outbound to subscribers) | Replaced by `clientCoordinator` |
| Replay goroutine in `hub.serveConn` | Replaced by file reader resuming from offset |
| `LastID` field sent by client on reconnect | Hub is authoritative; client no longer sends it |
| `lastReceivedFromHub` / `receiver` fields added to `Client` in earlier partial work | No longer needed; revert those changes |

### Added

| Item | Purpose |
|---|---|
| `internal/federation/channelreader.go` | Per-`(peer, channel)` file reader goroutine |
| `internal/federation/clientcoordinator.go` | Per-peer coordinator serialising sends across channels |
| `Hub.NotifyChannel(channel string)` | Wakes all channel readers for that channel |
| `Hub.notifyRegistry` | `map[string][]*channelReader` maintained by `serveConn` |
| `FedClientOffsetTTL time.Duration` on `HubConfig` | Configurable TTL for disconnected peer offset files; default 168h |
| TTL sweep goroutine on `Hub` | Deletes stale `fed-` offset files whose mtime exceeds TTL |

### Modified

| Item | Change |
|---|---|
| `hub.serveConn` | Creates `clientCoordinator` instead of `PeerSender` + replay goroutine for all connections with subscriptions |
| `messenger.go` `Publish` and `writeLocalEnvelope` | Call `hub.NotifyChannel(channel)` instead of `hub.EnqueueToAll(&env)` |
| `HandshakeMsg` | `LastID` field kept in struct (wire compat, `omitempty`) but hub ignores it and client stops sending it |
| `internal/federation/client.go` | Revert partial `lastReceivedFromHub`/`receiver` changes; stop sending `LastID` in handshake |
| `config.go` | Add `FedClientOffsetTTL` to `HubConfig` with default 168h |

### Unchanged

- `PeerReceiver` on hub (client→hub inbound messages)
- `PeerSender` on client (client→hub outbound messages)
- `PeerReceiver` on client (hub→client inbound messages — still receives batches)
- mTLS, allowlists, audit logging, wire framing, ack protocol
- `PeerReceiver.LastReceivedID()` added earlier — harmless, keep it

---

## New Components

### `channelReader`

**File:** `internal/federation/channelreader.go`

One instance per `(peer, subscribed channel)`. Lifecycle:

1. **Start:** Read stored offset from `subscribers/{channel}/fed-{peerName}.offset`. If the file does not exist (first connection), use the current end-of-channel offset so the subscriber receives only new messages.
2. **Watch:** Block on a `LocalNotifier`-style notification channel. The hub calls `NotifyChannel(channel)` after every write to that channel, which sends a non-blocking notification to all registered readers for that channel.
3. **Read:** On notification, scan from the stored offset through the segment files, accumulating envelopes up to `maxBatchBytes` into a single-channel batch. Track the byte offset of the end of the last envelope read.
4. **Send:** Put the batch and the new offset into the coordinator's `requestCh`.
5. **Ack:** Wait for the coordinator to confirm the ack was received (via a per-request `doneCh`).
6. **Persist:** Write the new byte offset to `subscribers/{channel}/fed-{peerName}.offset` using `storage.WriteOffset` (atomic: write to `.tmp`, fsync, rename).
7. **Loop:** Return to step 2.

On close: stop goroutine. Do **not** delete the offset file — it is needed by the compactor and the TTL sweep.

**Key fields:**
```
channelDir   string          // path to channel segment directory
offsetPath   string          // path to fed-{peer}.offset file
notifyCh     chan struct{}    // non-blocking, coalesced notifications
requestCh    chan<- sendReq   // shared with coordinator
log          logger
```

### `clientCoordinator`

**File:** `internal/federation/clientcoordinator.go`

One instance per connected peer. Owns N `channelReader`s (one per subscribed channel).

Runs a single send goroutine:
1. Wait for the next `sendReq` on `requestCh` (blocks until any channel reader has a batch ready).
2. Encode the batch using the existing `WriteFrame` / binary WebSocket framing.
3. Write to the WebSocket connection using `connWriteMu`.
4. Wait for the ack via `ackCh` (fed by the existing `PeerReceiver` ack-routing mechanism — unchanged).
5. Signal completion to the channel reader via `req.doneCh`.
6. Loop.

`sendReq` struct:
```
channel  string
envs     []*envelope.Envelope
newOffset int64
doneCh   chan<- struct{}
```

On disconnect: close `requestCh`, stop all channel readers, close the coordinator goroutine. Offset files remain on disk.

---

## Offset File Naming and Compaction

Offset files for federation peers are written to:

```
{dataDir}/subscribers/{channel}/fed-{peerName}.offset
```

The file format is identical to local subscriber offset files (a decimal integer followed by a newline). The compactor reads all files in `subscribers/{channel}/` and uses the minimum offset as the safe compaction boundary. No changes to the compactor are required — federation peers are automatically included.

The `fed-` prefix distinguishes these files from local subscriber files for logging and the TTL sweep; the compactor treats them identically.

---

## TTL Sweep

Connected peers update their offset files on every acked batch, so the file's mtime reflects the last time the peer was active. Peers that disconnect and never reconnect leave their offset files behind, which would otherwise block compaction indefinitely.

The hub runs a background TTL sweep goroutine (separate from the Messenger's compaction ticker) that:

1. Scans all `subscribers/{channel}/fed-*.offset` files across all channel directories under `dataDir`.
2. Compares each file's mtime to `time.Now() - FedClientOffsetTTL`.
3. Deletes files whose mtime is older than the TTL.
4. Logs each deletion at `Info` level with the peer name, channel, and age.

The sweep runs on a ticker. A reasonable interval is once per hour; the TTL is much longer so hourly checks are sufficient to keep things tidy.

**Config:**
```yaml
hub:
  fed_client_offset_ttl: 168h   # default: 1 week
```

Zero or negative TTL disables the sweep entirely.

---

## Notification Wiring

When a message is written to a channel (either via `Publish` or `writeLocalEnvelope`), the hub must wake all `channelReader`s registered for that channel.

`Hub` gains:
```go
notifyMu       sync.RWMutex
notifyRegistry map[string][]*channelReader  // channel → readers
```

`Hub.NotifyChannel(channel string)` acquires a read lock and sends a non-blocking notification to each registered reader:
```go
for _, r := range h.notifyRegistry[channel] {
    select {
    case r.notifyCh <- struct{}{}:
    default:
    }
}
```

In `serveConn`, after creating `channelReader`s, they are registered into `notifyRegistry` under a write lock. On disconnect, they are removed.

In `messenger.go`, `Publish` and `writeLocalEnvelope` call `hub.NotifyChannel(env.Channel)`. The existing `hub.EnqueueToAll` call is removed.

---

## Handshake Change

`LastID` is kept in `HandshakeMsg` with `omitempty` for wire compatibility with older peers. The hub ignores it entirely. The client stops sending it (passes `""` which omits the field from JSON).

---

## Implementation Steps

### Step 1 — Revert partial work
Revert the `lastReceivedFromHub` / `receiver` / `lastID`-direction changes in `internal/federation/client.go` added during the earlier investigation. Keep `PeerReceiver.LastReceivedID()` — it is harmless and may be useful later.

### Step 2 — Config
Add `FedClientOffsetTTL time.Duration` to `HubConfig` in `config.go`. Default: `168 * time.Hour`. Add to YAML parsing and `ApplyDefaults`.

### Step 3 — `channelReader`
Implement `internal/federation/channelreader.go`:
- `channelReader` struct and constructor
- `start()` — launches goroutine, loads or initialises offset
- `notify()` — non-blocking send to `notifyCh`
- `close()` — stops goroutine
- Internal read loop: open segments at offset, read batch, send to coordinator, wait for done, persist offset
- Unit tests: verify offset persistence, new-subscriber start-at-end behaviour, batch size limit, notification wake-up

### Step 4 — `clientCoordinator`
Implement `internal/federation/clientcoordinator.go`:
- `clientCoordinator` struct and constructor (takes conn, connWriteMu, ackCh, list of channelReaders)
- Single send goroutine
- `close()` — stops goroutine and all channel readers
- Unit tests: verify sequential delivery, ack propagation to reader, clean shutdown

### Step 5 — Hub wiring
Modify `internal/federation/hub.go`:
- Add `notifyRegistry` and `notifyMu`
- Add `NotifyChannel(channel string)` method
- Add TTL sweep goroutine started in `NewHub` (or `Listen`)
- Modify `serveConn`: for peers with subscriptions, create `clientCoordinator` with one `channelReader` per subscribed channel; register readers into `notifyRegistry`; on disconnect, deregister and close coordinator
- Remove `EnqueueToAll`, `ReplayFrom`, `channelSubscribers`, the replay goroutine
- Keep `PeerReceiver` creation in `serveConn` (inbound path unchanged)

### Step 6 — Messenger wiring
Modify `messenger.go`:
- In `Publish`: replace `hub.EnqueueToAll(&env)` with `hub.NotifyChannel(env.Channel)`
- In `writeLocalEnvelope`: replace `hub.EnqueueToAll(env)` with `hub.NotifyChannel(env.Channel)`

### Step 7 — Client cleanup
Modify `internal/federation/client.go`:
- Stop sending `LastID` in handshake (pass `""`)
- Confirm revert of partial changes from Step 1 is complete

### Step 8 — Compactor verification
Confirm the compactor's minimum-offset calculation reads all files in `subscribers/{channel}/` including `fed-` prefixed files. Add a focused test: create a channel with a local subscriber offset and a `fed-` prefixed offset file; assert `MaybeCompact` uses the minimum of both.

### Step 9 — Integration test
Add a test (in `integration_test.go` or a new `federation_restart_test.go` with build tag `integration`) that:
1. Starts a hub with a data directory
2. Connects a subscriber peer to two channels
3. Hub publishes N messages on each channel; subscriber receives all
4. Hub closes and restarts (new `Messenger` instance, same data directory)
5. Subscriber reconnects
6. Hub publishes M more messages
7. Assert: subscriber receives exactly M messages (no re-delivery of the first N)

### Step 10 — Remove stale tests
Remove or update any tests that relied on `ReplayFrom`, `EnqueueToAll`, or the old `LastID`-based replay behaviour.

### Step 11 — Run full test suite
```bash
go test -race ./...
go test -race -tags integration -timeout 60s ./...
```
All packages must show `ok`. Zero failures.

### Step 12 — Update `README.md` and `DESIGN.md`
- `DESIGN.md`: update the Federation section to describe the file-reader delivery model, offset file naming convention, TTL sweep, and removal of `ReplayFrom`/`LastID`. Remove references to `EnqueueToAll` and the push model.
- `README.md`: update any architecture overview or federation configuration sections that reference the old push behaviour or `LastID`.

---

## File Map Summary

```
internal/federation/
├── channelreader.go          NEW  per-(peer,channel) file reader
├── clientcoordinator.go      NEW  per-peer sequential send coordinator
├── hub.go                    MOD  remove EnqueueToAll/ReplayFrom; add NotifyChannel, TTL sweep
├── client.go                 MOD  stop sending LastID; revert partial earlier changes
├── handshake.go              MOD  LastID kept in struct but omitempty; no functional change
├── peersender.go             UNCHANGED (used by client→hub direction only)
├── peerreceiver.go           UNCHANGED (plus LastReceivedID() added earlier, kept)
├── policy.go                 UNCHANGED
└── framing.go                UNCHANGED

messenger.go                  MOD  NotifyChannel instead of EnqueueToAll
config.go                     MOD  FedClientOffsetTTL field on HubConfig

internal/storage/
└── compaction.go             VERIFY only (no expected changes)

DESIGN.md                     MOD  federation section rewrite
README.md                     MOD  architecture overview update
```
