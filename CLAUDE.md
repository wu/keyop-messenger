# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Status

Design phase. See `DESIGN.md` for the full architecture. No source code yet.

## Architecture Summary

Keyop Messenger is a Go pub-sub library with:
- Append-only `.jsonl` files as the durable message store (one file per channel)
- Per-subscriber byte-offset tracking for at-least-once delivery
- Federation via mTLS WebSocket connections between instances
- Star topology: clients connect to hubs; hub-to-hub forwarding is policy-driven (explicit channel lists, hot-reloaded)
- Audit log for all cross-hub forwarding events

Once source code is added, update this file with build/run/test commands and development setup.
