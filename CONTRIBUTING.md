# Contributing

Thanks for taking the time to contribute.

## Development setup

```bash
git clone https://github.com/anaxo-io/ws-fanout
cd ws-fanout
cargo test --features jwt
```

The toolchain is pinned in `rust-toolchain.toml`. Tests bind `127.0.0.1:0` and need no
other permissions. The `outcry` feature tests additionally need read access to the
[`outcry`](https://github.com/anaxo-io/outcry) repository and write access to the
system temp directory.

## Before opening a pull request

```bash
cargo fmt --all
cargo clippy --all-targets --features jwt -- -D warnings
cargo test --features jwt
cargo doc --no-deps --features jwt
```

If you have access to `outcry`, add `--features outcry,jwt` to the clippy and test lines.
CI runs all of these plus an MSRV check against Rust 1.89 and `cargo deny check`.

## Releasing

Releases follow the organisation procedure in
[`anaxo-io/.github` RELEASING.md](https://github.com/anaxo-io/.github/blob/main/RELEASING.md),
which this repository does not restate. The short version: write the changelog entry as
your change lands, then a maintainer cuts the release from `main` with `cargo release`,
driven by `release.toml` here. Nothing is published to crates.io.

## What this crate is careful about

- **The protocol is a contract.** `docs/protocol.md` is the specification; a change to
  what goes over the wire changes that file in the same pull request, and a change that
  breaks an existing client is a major version.
- **Publishing never blocks.** `Server::publish` takes a read lock and does `try_send`
  per subscriber. Nothing on that path may `await`, take a write lock, or allocate per
  subscriber. `tests/behaviour.rs::slow_client_drops_frames_and_publisher_never_blocks`
  is the guard.
- **Liveness is any inbound traffic.** The heartbeat tests model three clients — a
  browser answering Ping frames, a silent socket, and a JSON-only pinger — and all three
  outcomes are pinned. Do not make the server require a specific kind of reply.
- **No domain in here.** If a change mentions tiers, symbols, venues, or a particular
  payload shape, it belongs in the application, not the crate. Add a trait hook instead.
- **`#![forbid(unsafe_code)]`.** Stays.

## Reporting bugs

Open an issue with the smallest client interaction that reproduces it — ideally as a
test in `tests/behaviour.rs`, which already has helpers for connect, auth and subscribe.
