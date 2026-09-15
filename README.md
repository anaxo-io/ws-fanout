# ws-fanout

[![CI](https://github.com/anaxo-io/ws-fanout/actions/workflows/ci.yml/badge.svg)](https://github.com/anaxo-io/ws-fanout/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](https://blog.rust-lang.org/)

A WebSocket fan-out server for market data and anything else shaped like it: publish a
message on a named channel once, every subscriber gets it. Token auth, per-channel
authorisation, snapshot-on-subscribe, heartbeats that work with browsers, and a slow
client that hurts only itself.

## Why

The pub/sub edge of a data pipeline is small and everyone writes it badly the first time.
The three mistakes this crate exists to not make:

- **Browsers get dropped.** A server that sends a JSON `ping` and waits for a WebSocket
  *Pong frame* closes every browser at the first timeout: browsers answer Ping *frames*,
  automatically, and never send Pong frames on their own. This server pings with a frame
  and counts any inbound traffic as liveness.
- **One slow client stalls the publisher.** Awaiting a send to a client whose TCP window
  is closed blocks the loop for everyone. Here every connection has a bounded queue; a
  full queue drops that client's frame and counts it, and the publisher never waits.
- **The payload is serialised once per client.** A tick to a thousand subscribers should
  be one serialisation and a thousand pointer copies, not a thousand serialisations.
  Payloads are opaque JSON embedded verbatim into a frame that is built once and shared.

The server knows nothing about your domain. Channels are strings, payloads are JSON,
tokens are validated by a trait you implement (an HS256 JWT validator ships behind the
`jwt` feature), authorisation is a closure. There are no user tiers in here.

## Quick start

```toml
[dependencies]
ws-fanout = { git = "https://github.com/anaxo-io/ws-fanout", tag = "v0.1.0" }
```

```rust
use ws_fanout::{Server, StaticToken};

#[tokio::main]
async fn main() -> ws_fanout::Result<()> {
    let server = Server::builder()
        .validator(StaticToken::new("demo-token", "demo"))
        .bind("127.0.0.1:9001")
        .await?;

    // From any thread, as often as you like. Never blocks.
    server.publish_json("ticker:BTC-USD", &serde_json::json!({ "last": 79134.01 }));
    Ok(())
}
```

A client, in a browser console:

```js
const ws = new WebSocket("ws://127.0.0.1:9001");
ws.onopen = () => ws.send(JSON.stringify({ type: "auth", token: "demo-token" }));
ws.onmessage = (e) => {
  const m = JSON.parse(e.data);
  if (m.type === "authenticated") ws.send(JSON.stringify({ type: "subscribe", channels: ["ticker:BTC-USD"] }));
  if (m.type === "data") console.log(m.channel, m.payload);
};
```

The full wire protocol is in [`docs/protocol.md`](docs/protocol.md).

## Features

- **Explicit auth acknowledgement.** `authenticated` is the first thing a client
  receives, so it knows when to subscribe instead of guessing or racing.
- **Snapshot on subscribe.** Implement `SnapshotSource` (or use the bundled `LastValue`)
  and each new subscriber gets current state before its first delta.
- **Per-channel authorisation.** `Authorizer` is called for every requested channel with
  the token's claims; refused channels come back in the `subscribed` ack with a reason.
- **Resync signalling.** `Server::resync_all()` tells every client to discard state — the
  right thing to do when the upstream feed drops data.
- **Limits that are actually enforced.** Connections, message size, subscriptions per
  connection, queue depth, consecutive drops. See the table in the protocol doc.
- **Metrics.** Atomic counters for connections, timeouts, refusals, frames sent and
  dropped. Export them with whatever you already use.
- **A shared-memory input.** With the `outcry` feature, `Server::consume_outcry` drains
  an [`outcry`](https://github.com/anaxo-io/outcry) broadcast queue on a dedicated thread,
  so the process producing the data and the process serving it can be different ones. An
  overrun on the queue becomes a `resync` to every client.

## Non-goals

- **No TLS.** Terminate it in front of this — nginx, Caddy, a load balancer. The crate
  has no TLS dependency and `cargo deny` refuses `openssl`.
- **No clustering.** One process, one registry. Fan out across processes with `outcry`
  or your own bus and run several of these behind a balancer.
- **No message history or replay.** A `snapshot` is the only catch-up mechanism.
- **No compression, no binary payloads.** Text frames with JSON. If you need a compact
  wire format, this is the wrong layer.

## Examples

```bash
# A queue writer and a gateway reading it, in two terminals:
cargo run --features outcry --example synthetic_feed
cargo run --features outcry --example gateway
```

Then connect a browser with the snippet above using token `demo`.

## Performance

Publishing is one `serde_json` serialisation, one `Arc<str>` allocation, a read lock on
the registry, and one `try_send` per subscriber. The measured cost is dominated by the
kernel: a `Message::Text` send per connection. This release ships no benchmark harness;
the number that matters is subscribers per core at a given message rate, and that needs
a load generator. It is tracked in [#1](https://github.com/anaxo-io/ws-fanout/issues/1).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Bug reports with a failing test are especially
welcome.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for
inclusion in this crate by you, as defined in the Apache-2.0 license, shall be dual licensed
as above, without any additional terms or conditions.
