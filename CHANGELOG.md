## [v0.6.0](https://github.com/wu/keyop-messenger/compare/v0.5.0...v0.6.0) (2026-04-10)

### Chores

* update release tag format to include "v" prefix ([2047d3d](https://github.com/wu/keyop-messenger/commit/2047d3d0a9a1e15ba1dd1e09c69b44f9fcbb6e90))

## [v0.5.0](https://github.com/wu/keyop-messenger/compare/v0.4.0...v0.5.0) (2026-04-10)

### Features

* add InstanceName method and tests ([c025026](https://github.com/wu/keyop-messenger/commit/c0250269aa6efc25db593f4296406add485a0a0f))

## [v0.4.0](https://github.com/wu/keyop-messenger/compare/v0.3.0...v0.4.0) (2026-04-09)

### Features

* add Ephemeral Client support with live-only messaging and stateless operation ([9ae9c40](https://github.com/wu/keyop-messenger/commit/9ae9c40f2f38f32197f3735a9da71d3f2f7584d3))

### Refactoring

* clean up failing lint tests ([20f8ad6](https://github.com/wu/keyop-messenger/commit/20f8ad66c16a99d02f56c7bf0ff430823a3d76a1))

## [v0.3.0](https://github.com/wu/keyop-messenger/compare/v0.2.0...v0.3.0) (2026-04-09)

### Features

* add godoc badge, testing semantic release workflow ([c737dd2](https://github.com/wu/keyop-messenger/commit/c737dd2c2896e9587cb3ef99835e29645be4499c))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.2.0] - 2026-04-09

### Added
- Federation support with mTLS WebSocket connections between instances
- Star topology hub-client architecture
- Per-client channel allowlists
- Audit logging for cross-hub forwarding events
- Policy hot-reloading for federation rules

### Changed
- Enhanced subscriber offset tracking for distributed scenarios

## [v0.1.0] - 2026-04-09

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
