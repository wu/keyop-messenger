## [0.12.0](https://github.com/wu/keyop-messenger/compare/v0.11.0...v0.12.0) (2026-04-24)

### Features

* enhance audit logging for client connection and disconnection events ([e773f72](https://github.com/wu/keyop-messenger/commit/e773f72c9f918948aa71fab2b16ea275862b30b8))

## [0.11.0](https://github.com/wu/keyop-messenger/compare/v0.10.1...v0.11.0) (2026-04-15)

### Features

* update federated clients to work more like local clients ([7d05a1a](https://github.com/wu/keyop-messenger/commit/7d05a1a0319a42ca6de3882c7ddaee6634f82223))

## [0.10.1](https://github.com/wu/keyop-messenger/compare/v0.10.0...v0.10.1) (2026-04-13)

### Bug Fixes

* resolve issue with publish channel support and client receive policy ([23a251f](https://github.com/wu/keyop-messenger/commit/23a251f2e3171dab59d2f1d7160cef8b5a89f893))

## [0.10.0](https://github.com/wu/keyop-messenger/compare/v0.9.1...v0.10.0) (2026-04-13)

### Features

* add ServiceName context field to track message publishers ([2d82aba](https://github.com/wu/keyop-messenger/commit/2d82abaf35126066616e0b81cf237fa4dd86a14a))

## [0.9.1](https://github.com/wu/keyop-messenger/compare/v0.9.0...v0.9.1) (2026-04-13)

### Bug Fixes

* resolve a bug with client channel filtering ([70791ba](https://github.com/wu/keyop-messenger/commit/70791ba885845664501bbd15f22ec9d4a50d8d17))

## [0.9.0](https://github.com/wu/keyop-messenger/compare/v0.8.0...v0.9.0) (2026-04-11)

### Features

* add support for correlation IDs for message tracing ([860b61f](https://github.com/wu/keyop-messenger/commit/860b61f9a69aa4468db4fd8d4ee2405167730f7e))

## [0.8.0](https://github.com/wu/keyop-messenger/compare/v0.7.0...v0.8.0) (2026-04-11)

### Features

* optimize peer message routing with reverse index and add cleanup tests ([acacb00](https://github.com/wu/keyop-messenger/commit/acacb00a0bcc310cfeae039d05684bdff0bd556c))
* remove hot-reload logic and related tests for allowlist policies ([f148423](https://github.com/wu/keyop-messenger/commit/f14842311351f90d93403ec4260df1b060b09cdb))

## [0.7.0](https://github.com/wu/keyop-messenger/compare/v0.6.2...v0.7.0) (2026-04-11)

### Features

* add client-to-client message forwarding test and improve hub forwarding logic ([7b2fad6](https://github.com/wu/keyop-messenger/commit/7b2fad6082bc6b0dffa8099b21c0bdacdc04bbb9))

### Bug Fixes

* add shared mutex to synchronize concurrent writes to WebSocket connections ([ab87f8f](https://github.com/wu/keyop-messenger/commit/ab87f8fc9f6c10a97559ab36ff3c7ca608f6741e))

## [0.6.2](https://github.com/wu/keyop-messenger/compare/v0.6.1...v0.6.2) (2026-04-10)

### Refactoring

* unify client and peer configurations under a single `allowed_peers` abstraction ([76dd0cf](https://github.com/wu/keyop-messenger/commit/76dd0cf51b7ff1f53d089d44dbd2e9eb4aeaecb8))

## [0.6.1](https://github.com/wu/keyop-messenger/compare/v0.6.0...v0.6.1) (2026-04-10)

### Bug Fixes

* return context.Canceled error if dial context is canceled ([770ea1d](https://github.com/wu/keyop-messenger/commit/770ea1d71439cd802f9ee65bd726219e6e6520c4))

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
