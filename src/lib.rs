//! A WebSocket fan-out server: publish once, every subscriber gets it.
//!
//! Clients connect, present a token, subscribe to channels by name, and receive every
//! message published on those channels as JSON text frames. The server does not know what
//! a channel means or what a payload contains — channels are strings and payloads are
//! opaque JSON, embedded verbatim and serialised **once per publish**, so a message to a
//! thousand subscribers costs one serialisation and a thousand queue pushes.
//!
//! What it gets right that is easy to get wrong:
//!
//! - **Heartbeats that work with browsers.** The server sends WebSocket Ping *frames* and
//!   counts any inbound traffic as liveness. Browsers answer Ping frames automatically and
//!   never send Pong frames unprompted; a server that pings in JSON and waits for a Pong
//!   frame drops every browser at its first timeout.
//! - **Slow clients pay, nobody else does.** Each connection has a bounded queue; a full
//!   queue drops the frame for that client and counts it. The publisher never blocks. A
//!   client that keeps dropping is closed.
//! - **An acknowledgement to wait for.** `authenticated` is sent before anything else, so a
//!   client knows when it may subscribe instead of guessing.
//! - **Nothing about your domain.** Token validation and authorisation are traits; the
//!   defaults accept a fixed token and every channel. There are no tiers in here.
//!
//! # Example
//!
//! ```no_run
//! use ws_fanout::{Server, StaticToken};
//!
//! # async fn run() -> ws_fanout::Result<()> {
//! let server = Server::builder()
//!     .validator(StaticToken::new("demo-token", "demo"))
//!     .bind("127.0.0.1:9001")
//!     .await?;
//!
//! // From any thread, as often as you like:
//! server.publish_json("ticker:BTC-USD", &serde_json::json!({ "last": 79134.01 }));
//! # Ok(())
//! # }
//! ```
//!
//! A client, in JavaScript:
//!
//! ```js
//! const ws = new WebSocket("ws://127.0.0.1:9001");
//! ws.onopen = () => ws.send(JSON.stringify({ type: "auth", token: "demo-token" }));
//! ws.onmessage = (e) => {
//!   const m = JSON.parse(e.data);
//!   if (m.type === "authenticated") ws.send(JSON.stringify({ type: "subscribe", channels: ["ticker:BTC-USD"] }));
//!   if (m.type === "data") console.log(m.channel, m.payload);
//! };
//! ```
//!
//! The protocol is specified in `docs/protocol.md`. With the `outcry` feature the server
//! can be fed from a shared-memory queue on a dedicated thread; see
//! [`Server::consume_outcry`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod auth;
pub mod error;
pub mod protocol;
pub mod registry;
pub mod server;
pub mod snapshot;

#[cfg(feature = "outcry")]
pub mod outcry_source;

use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::value::RawValue;
use tokio::net::{TcpListener, ToSocketAddrs};

#[cfg(feature = "jwt")]
pub use auth::HmacJwt;
pub use auth::{AllowAll, Authorizer, Claims, StaticToken, TokenValidator};
pub use error::{Error, Result};
pub use protocol::{ClientMessage, Frame, ServerMessage};
pub use registry::Metrics;
pub use server::Config;
pub use snapshot::{LastValue, NoSnapshots, SnapshotSource};

/// A running server. Clone it to publish from anywhere; drop every clone to stop.
#[derive(Clone)]
pub struct Server {
    shared: Arc<server::Shared>,
    addr: SocketAddr,
    /// Source threads hold a `Weak` to this; when the last `Server` clone goes, they stop.
    alive: Arc<()>,
}

impl Server {
    /// Start configuring a server.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// The address the listener is bound to. Useful when bound to port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Counters.
    pub fn metrics(&self) -> &Arc<Metrics> {
        self.shared.registry.metrics()
    }

    /// Open connections.
    pub fn connections(&self) -> usize {
        self.shared.registry.len()
    }

    /// Subscribers to `channel`.
    pub fn subscribers(&self, channel: &str) -> usize {
        self.shared.registry.subscribers(channel)
    }

    /// Publish `payload` on `channel`. Serialised once; delivered to every subscriber
    /// whose queue has room. Returns how many received it. Never blocks.
    pub fn publish(&self, channel: &str, payload: &RawValue) -> usize {
        let frame = ServerMessage::Data { channel, payload }.to_frame();
        self.shared.registry.publish(channel, &frame)
    }

    /// [`publish`](Self::publish) for a value that is not yet JSON text.
    pub fn publish_json<T: serde::Serialize>(&self, channel: &str, value: &T) -> usize {
        match serde_json::value::to_raw_value(value) {
            Ok(raw) => self.publish(channel, &raw),
            Err(e) => {
                tracing::error!(channel, error = %e, "payload did not serialise");
                0
            }
        }
    }

    /// Tell every client that the upstream lost data and everything it holds may be
    /// stale. Sent to all connections, subscribed or not.
    pub fn resync_all(&self) -> usize {
        let frame = ServerMessage::Resync { channels: vec![] }.to_frame();
        self.shared.registry.broadcast(&frame)
    }

    /// Tell subscribers of `channels` that those channels lost data.
    pub fn resync(&self, channels: &[&str]) -> usize {
        let frame = ServerMessage::Resync {
            channels: channels.to_vec(),
        }
        .to_frame();
        let mut n = 0;
        for ch in channels {
            n += self.shared.registry.publish(ch, &frame);
        }
        n
    }
}

/// Configures and starts a [`Server`].
pub struct Builder {
    config: Config,
    validator: Box<dyn TokenValidator>,
    authorizer: Box<dyn Authorizer>,
    snapshots: Box<dyn SnapshotSource>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            config: Config::default(),
            validator: Box::new(StaticToken::new("", "anonymous")),
            authorizer: Box::new(AllowAll),
            snapshots: Box::new(NoSnapshots),
        }
    }
}

impl Builder {
    /// Replace the whole [`Config`].
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// How tokens are validated. Default: a [`StaticToken`] accepting the empty string.
    pub fn validator(mut self, v: impl TokenValidator) -> Self {
        self.validator = Box::new(v);
        self
    }

    /// Which channels a connection may subscribe to. Default: all.
    pub fn authorizer(mut self, a: impl Authorizer) -> Self {
        self.authorizer = Box::new(a);
        self
    }

    /// Where snapshots come from on subscribe. Default: none.
    pub fn snapshots(mut self, s: impl SnapshotSource) -> Self {
        self.snapshots = Box::new(s);
        self
    }

    /// Ping interval and the silence after which a client is dropped.
    pub fn heartbeat(
        mut self,
        interval: std::time::Duration,
        timeout: std::time::Duration,
    ) -> Self {
        self.config.heartbeat_interval = interval;
        self.config.heartbeat_timeout = timeout;
        self
    }

    /// Refuse connections beyond this many.
    pub fn max_connections(mut self, n: usize) -> Self {
        self.config.max_connections = n;
        self
    }

    /// Bind and start accepting. The accept loop runs on the current tokio runtime.
    pub async fn bind(self, addr: impl ToSocketAddrs) -> Result<Server> {
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(server::Shared {
            config: self.config,
            registry: registry::Registry::new(Arc::new(Metrics::default())),
            validator: self.validator,
            authorizer: self.authorizer,
            snapshots: self.snapshots,
        });
        tokio::spawn(server::serve(listener, Arc::clone(&shared)));
        Ok(Server {
            shared,
            addr,
            alive: Arc::new(()),
        })
    }
}
