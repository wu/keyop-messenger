# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

Phases 1–14 complete. Phase 15 (hardening, benchmarks, integration tests) is next.
See `DESIGN.md` for the full architecture and `IMPLEMENTATION.md` for the phase plan.

## Build / Test / Install Commands

```bash
go test -race ./...                                              # run all unit tests
go test -race -v ./...                                          # verbose
go build ./...                                                  # build library + CLI
go install ./cmd/keyop-messenger                                # install CLI binary
go mod tidy                                                     # sync dependencies
```

## Architecture Summary

Keyop Messenger is a Go pub-sub library with:
- Append-only `.jsonl` segment files as the durable message store (one directory per channel)
- Per-subscriber byte-offset tracking for at-least-once delivery
- Federation via mTLS WebSocket connections between instances
- Star topology: clients connect to hubs; hub-to-hub forwarding is policy-driven (explicit channel lists, hot-reloaded)
- Audit log (`{data_dir}/audit/audit.jsonl`) for all cross-hub forwarding events
- `keyop-messenger keygen ca` / `keygen instance` CLI for certificate generation

## Key Packages

| Package | Role |
|---|---|
| `github.com/keyop/keyop-messenger` | Public API (`Messenger`, `New`, `Publish`, `Subscribe`, `Close`) |
| `internal/storage` | Writer goroutine, subscriber goroutine, offset files, compaction |
| `internal/federation` | Hub, Client, PeerSender, PeerReceiver, policy hot-reload |
| `internal/envelope` | Envelope struct, marshal/unmarshal, payload registry |
| `internal/tlsutil` | TLS config builder, cert generation, expiry checks |
| `internal/audit` | Audit writer with rotation |
| `internal/dedup` | LRU seen-ID deduplication |
| `cmd/keyop-messenger` | CLI entry point (`keygen ca`, `keygen instance`, `version`) |
