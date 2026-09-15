# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-09-15

### Fixed

- CI: every job, not only the `outcry` one, failed to resolve the private `outcry` git
  dependency, because cargo fetches each git source in the manifest whether or not its
  feature is enabled. The jobs now strip that dependency before building, until `outcry`
  is public ([#3](https://github.com/anaxo-io/ws-fanout/issues/3)). The v0.1.0 tag has a
  red CI run for this reason and no other; the code is unchanged.

## [0.1.0] - 2026-09-15

Initial release.

### Added

- `Server` with a builder: token validation (`TokenValidator`), per-channel authorisation
  (`Authorizer`), snapshot-on-subscribe (`SnapshotSource`), heartbeat and limit tuning.
- Wire protocol (`docs/protocol.md`): `auth` → `authenticated`, `subscribe` with
  per-channel rejections, `snapshot`, `data`, `resync`, `ping`/`pong`, `error`.
- Fan-out with one serialisation per publish; bounded per-connection queues that drop
  and count rather than block.
- Heartbeat by WebSocket Ping frame with any inbound traffic as liveness, so browser
  clients survive without application code.
- `Metrics` counters.
- `jwt` feature: `HmacJwt`, an HS256 validator requiring `sub` and `exp`.
- `outcry` feature: `Server::consume_outcry` drains a shared-memory broadcast queue on a
  dedicated thread; an overrun becomes a `resync` to every client.
- Examples: `gateway` and `synthetic_feed` (both need `--features outcry`).

### Known issues

- No benchmark or load generator yet; the README makes no throughput claim
  ([#1](https://github.com/anaxo-io/ws-fanout/issues/1)).
- The accept loop runs until the tokio runtime stops; dropping the last `Server` stops
  source threads but not the listener
  ([#2](https://github.com/anaxo-io/ws-fanout/issues/2)).
- The `outcry` feature's CI job is advisory while `outcry` is a private git dependency
  ([#3](https://github.com/anaxo-io/ws-fanout/issues/3)).

[Unreleased]: https://github.com/anaxo-io/ws-fanout/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/anaxo-io/ws-fanout/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/anaxo-io/ws-fanout/releases/tag/v0.1.0
