## [0.3.0](https://github.com/wu/keyop-messenger/compare/v0.2.0...0.3.0) (2026-04-09)

### Features

* add godoc badge, testing semantic release workflow ([c737dd2](https://github.com/wu/keyop-messenger/commit/c737dd2c2896e9587cb3ef99835e29645be4499c))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-04-09

### Added
- Federation support with mTLS WebSocket connections between instances
- Star topology hub-client architecture
- Per-client channel allowlists
- Audit logging for cross-hub forwarding events
- Policy hot-reloading for federation rules

### Changed
- Enhanced subscriber offset tracking for distributed scenarios

## [0.1.0] - 2026-04-09

### Added
- Initial public release
- Append-only `.jsonl` segment files for durable message storage
- At-least-once delivery semantics
- Persistent offset tracking for subscribers
- Type-safe payload registry
- Configurable retry logic with dead-letter queue support
- Low-latency dual-layer notification system (LocalNotifier + fsnotify)
- File-based observability via standard Unix tools
- CLI tool for key generation (`keygen ca`, `keygen instance`)
