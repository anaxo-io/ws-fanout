# CLAUDE.md

Guidance for Claude Code working in this repository.

## What this is

`ws-fanout` is a WebSocket fan-out server: clients authenticate, subscribe to named
channels, and receive every message published on them. Channels are strings, payloads are
opaque JSON embedded verbatim, and a payload is serialised **once per publish** no matter
how many subscribers there are.

Extracted from a private trading monorepo's API gateway. The crate knows nothing about
trading: no symbols, no venues, no user tiers. Token validation, authorisation and
snapshots are traits the application implements.

## Quality gates

```bash
cargo fmt --all
cargo clippy --all-targets --features jwt -- -D warnings
cargo test --features jwt
cargo doc --no-deps --features jwt
```

With access to `outcry`, add `--features outcry,jwt` to the clippy and test lines. CI runs
the above plus an MSRV check against Rust 1.89 and `cargo deny check`.

## What this crate is careful about

`CONTRIBUTING.md` states the review rules, and `docs/protocol.md` is the wire
specification — a change to what goes over the wire changes that file in the same pull
request. The three that matter most:

- **Heartbeats are WebSocket Ping *frames*, and any inbound traffic counts as liveness.**
  Browsers answer Ping frames automatically and never send Pong frames unprompted, so a
  server that pings in JSON and waits for a Pong frame drops every browser at its first
  timeout. That was the bug in the original gateway. Three tests model a browser, a silent
  socket and a JSON-only pinger, and all three outcomes are pinned.
- **Publishing never blocks.** `Server::publish` takes a read lock and does one
  `try_send` per subscriber. Nothing on that path may `await`, take a write lock, or
  allocate per subscriber. A full queue drops that client's frame and counts it; a client
  that keeps dropping is closed.
- **No domain in here.** A change that mentions tiers, symbols, venues or a particular
  payload shape belongs in the application. Add a trait hook instead.

Also: `#![forbid(unsafe_code)]` stays, and the `authenticated` acknowledgement is part of
the contract — clients wait for it before subscribing.

## The outcry dependency, and why CI looks odd

The optional `outcry` feature depends on the private `anaxo-io/outcry` repository by git
tag. Cargo resolves **every** git source in a manifest whether or not its feature is
enabled, and a workflow's `GITHUB_TOKEN` can only read its own repository, so without help
all jobs fail rather than just the feature job. Two workarounds are in place:

1. Each required job strips the dependency line from `Cargo.toml` with `sed` before
   building. Keep the `outcry = []` feature declaration when stripping, or
   `cfg(feature = "outcry")` becomes an unknown-value error, and note that
   `autoexamples = false` exists so the stripped example blocks are not re-discovered.
2. The job that does enable the feature is `continue-on-error` and is not a required
   check.

Locally, `.cargo/config.toml` sets `git-fetch-with-cli = true` so cargo uses the `gh`
credential helper.

**When `outcry` becomes public or is published to crates.io**, delete the strip steps,
drop the `continue-on-error`, add `test (outcry feature)` to branch protection, and close
issue #3. A breaking change in `outcry` means a coordinated tag bump here.

## Repository conventions

- Dual licensed MIT OR Apache-2.0. Fresh history; never reference the monorepo it came
  from, by name or by path.
- `CHANGELOG.md` follows Keep a Changelog. **Dependency bumps get an entry too**, saying
  what the upgrade needed rather than just the version pair.
- Conventional-commit subjects. Do not commit unless Hicham asks.
- Run `gitleaks protect --staged` before every push.
- Dependabot must not bump `dtolnay/rust-toolchain`: that tag names a Rust release, not an
  action version.
- `gh pr checks` returns nothing for these repos. Read status from
  `gh run list --json conclusion` and `gh run view <id> --json jobs` instead.
- Renaming a CI job renames its status check, and branch protection matches contexts by
  name. Repatch it in the same change or the old context waits forever — silently, until
  the day pull requests are required. The `msrv (1.88)` → `msrv (1.89)` rename hit this:

  ```bash
  gh api repos/anaxo-io/ws-fanout/branches/main/protection/required_status_checks
  gh api -X PATCH repos/anaxo-io/ws-fanout/branches/main/protection/required_status_checks \
    --input -   # {"strict":true,"contexts":[...]}
  ```
