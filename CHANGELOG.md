# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- CI builds the manifest as written. `outcry` is public now, so the five jobs that
  stripped the git dependency with `sed` before building no longer need to, and the
  `autoexamples = false` that stopped the stripped example blocks being rediscovered is
  gone with them. The `test (outcry feature)` job is no longer advisory: it is a required
  check like the others ([#3](https://github.com/anaxo-io/ws-fanout/issues/3)).
- Slow-client eviction now counts the drops it was meant to count. Only a failed reply to
  an inbound message was recorded, and the counter was never reset, so it was a lifetime
  total rather than a streak — fan-out drops, the only kind a genuinely slow client
  produces, never counted at all and such a client was never closed. Every delivery path
  updates one per-connection streak, cleared by the first frame that gets through.
- Sending a frame no longer copies its JSON once per subscriber. `Frame` held an
  `Arc<str>`, and tungstenite's `Utf8Bytes: From<&str>` is `Bytes::copy_from_slice`, so
  "serialised once" was true and then undone at the sink. `Frame` now holds the buffer
  type the sink takes, and sending is a refcount bump.
- Socket writes have a deadline. Each one happens inside a `select!` branch and a branch
  body runs to completion, so a peer that stopped reading parked the heartbeat, the
  inbound reads and the slow-client check for as long as it liked.
- `max_connections` is a semaphore permit taken before the handshake and held for the
  connection's life. It counted registered connections, which excluded everything still
  handshaking or authenticating; the handshake had no timeout of its own either, and now
  shares the auth timeout.
- A connection is removed from the registry by a guard, so a panic in an `Authorizer`,
  `TokenValidator` or `SnapshotSource` no longer leaks its entry and its permit forever.
- Dropping every `Server` clone stops the accept loop. The doc said so; the `JoinHandle`
  was discarded ([#2](https://github.com/anaxo-io/ws-fanout/issues/2)).
- `authenticated` is written to the socket before the connection enters the registry, so
  a concurrent `resync_all` or `broadcast` cannot overtake the acknowledgement clients are
  told to wait for.

### Added

- A release workflow and a `release.toml`, following the organisation procedure in
  [`anaxo-io/.github` RELEASING.md](https://github.com/anaxo-io/.github/blob/main/RELEASING.md)
  rather than carrying a copy of it. Pushing a `vX.Y.Z` tag calls the shared workflow,
  which checks the tag against the crate version and cuts the GitHub release from that
  version's changelog section. It is passed `package: false`, because `cargo package`
  resolves every dependency against crates.io whatever the features say and the `outcry`
  feature is a git source; the `cargo check --all-features` it runs instead needs to read
  that private repository, so releases stay blocked until outcry is public (#3).
- `Config::max_channel_len` (default 256 bytes). Channel names were bounded in number by
  `max_subscriptions` but not in size, so one connection could retain megabytes of names.
  Over-long names are reported as `rejected/invalid`.
- `Builder::bind` validates `Config` and returns the new `Error::Config` instead of
  letting a zero `send_queue` or `heartbeat_interval` panic inside tokio later, on a task
  the caller cannot catch.

### Changed

- **Breaking.** `Frame`'s field is private. It was `pub Arc<str>`, which would have made
  the per-subscriber copy fix a breaking change after publication.
- **Breaking.** `Error`, `ClientMessage` and `ServerMessage` are `#[non_exhaustive]`, so a
  new error or wire message is no longer a breaking change.
- **Breaking.** `Error::Full` is gone. Connections over the limit are now refused in the
  accept loop, so the variant was unreachable.
- **Breaking.** MSRV is now Rust 1.89, up from 1.88. `outcry` v0.2.0 declares 1.89, so
  keeping 1.88 here would have been a claim only the jobs that strip the dependency could
  honour — the `msrv` job strips it, so it would have passed green while the `outcry`
  feature was broken for anyone on 1.88.
- `outcry` v0.1.0 to v0.2.0. No source change beyond the MSRV bump above; the consumer
  API this crate uses is unchanged.

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
