//! The listener and the per-connection state machine.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use tracing::{debug, info, warn};

use crate::auth::{Authorizer, Claims, TokenValidator};
use crate::error::{Error, Result};
use crate::protocol::{ClientMessage, Frame, Rejected, ServerMessage};
use crate::registry::{ConnectionId, Registry};
use crate::snapshot::SnapshotSource;

/// Tunables. Built through [`Server::builder`](crate::Server::builder).
#[derive(Debug, Clone)]
pub struct Config {
    /// Refuse new connections beyond this many.
    pub max_connections: usize,
    /// Largest inbound WebSocket message accepted; larger ones close the connection.
    pub max_message_size: usize,
    /// Channels one connection may hold.
    pub max_subscriptions: usize,
    /// Longest channel name accepted; longer ones are rejected as `invalid`. Without it,
    /// `max_subscriptions` bounds the number of retained names but not their size.
    pub max_channel_len: usize,
    /// Frames queued per connection before drops begin.
    pub send_queue: usize,
    /// Consecutive drops after which a connection is closed as too slow.
    pub max_consecutive_drops: u64,
    /// How long a client has to complete the WebSocket handshake and send `auth`. It
    /// covers the handshake too, so a connection that never upgrades is not free.
    pub auth_timeout: Duration,
    /// How often the server pings.
    pub heartbeat_interval: Duration,
    /// Silence after which a connection is closed. Doubles as the deadline for a single
    /// socket write: a write still pending after it means the same thing as silence.
    pub heartbeat_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connections: 10_000,
            max_message_size: 64 * 1024,
            max_subscriptions: 256,
            max_channel_len: 256,
            send_queue: 256,
            max_consecutive_drops: 1_000,
            auth_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(90),
        }
    }
}

impl Config {
    /// Reject values that would panic inside tokio, before anything is bound or spawned.
    pub(crate) fn validate(&self) -> Result<()> {
        let bad = |field| Err(Error::Config(field));
        match self {
            c if c.max_connections == 0 => bad("max_connections must be non-zero"),
            c if c.max_connections > Semaphore::MAX_PERMITS => {
                bad("max_connections exceeds the permit limit")
            }
            c if c.max_message_size == 0 => bad("max_message_size must be non-zero"),
            c if c.max_subscriptions == 0 => bad("max_subscriptions must be non-zero"),
            c if c.max_channel_len == 0 => bad("max_channel_len must be non-zero"),
            c if c.send_queue == 0 => bad("send_queue must be non-zero"),
            c if c.heartbeat_interval.is_zero() => bad("heartbeat_interval must be non-zero"),
            _ => Ok(()),
        }
    }
}

/// Everything a connection task needs, shared.
pub(crate) struct Shared {
    pub config: Config,
    pub registry: Registry,
    pub validator: Box<dyn TokenValidator>,
    pub authorizer: Box<dyn Authorizer>,
    pub snapshots: Box<dyn SnapshotSource>,
    /// One permit per allowed connection, held from before the handshake until the
    /// connection task ends. Counting registered connections instead would leave every
    /// client still handshaking or authenticating outside the limit.
    pub slots: Arc<Semaphore>,
}

/// Removes a connection from the registry however its task ends, panic included.
struct Registered<'a> {
    registry: &'a Registry,
    id: ConnectionId,
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.registry.remove(self.id);
    }
}

/// Accept loop. Runs until the listener fails.
pub(crate) async fn serve(listener: TcpListener, shared: Arc<Shared>) -> Result<()> {
    info!(addr = %listener.local_addr()?, "listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        let Ok(permit) = Arc::clone(&shared.slots).try_acquire_owned() else {
            shared
                .registry
                .metrics()
                .refused_full
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            debug!(%peer, "refused: at the connection limit");
            continue;
        };
        let shared = Arc::clone(&shared);
        tokio::spawn(async move {
            let _permit = permit;
            match connection(stream, peer, shared).await {
                Ok(()) | Err(Error::WebSocket(_)) => {}
                Err(e) => debug!(%peer, error = %e, "connection ended"),
            }
        });
    }
}

async fn connection(stream: TcpStream, peer: SocketAddr, shared: Arc<Shared>) -> Result<()> {
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(shared.config.max_message_size))
        .max_frame_size(Some(shared.config.max_message_size));
    // The handshake is on the auth clock too: a client that opens a socket and never
    // completes the upgrade would otherwise hold its permit forever.
    let mut ws = tokio::time::timeout(
        shared.config.auth_timeout,
        tokio_tungstenite::accept_async_with_config(stream, Some(ws_config)),
    )
    .await
    .map_err(|_| Error::Unauthenticated("handshake timeout".into()))??;

    // Authentication: the first text frame must be `auth`, within the timeout.
    let claims = match authenticate(&mut ws, &shared).await {
        Ok(c) => c,
        Err(e) => {
            let frame = ServerMessage::Error {
                code: "unauthenticated",
                message: e.to_string(),
            }
            .to_frame();
            let _ = ws.send(frame.to_message()).await;
            let _ = ws.close(None).await;
            return Err(e);
        }
    };

    // The acknowledgement the client waits for before subscribing. It goes out on the
    // socket before the connection joins the registry, so no broadcast can overtake it.
    let ack = ServerMessage::Authenticated {
        subject: &claims.subject,
    }
    .to_frame();
    ws.send(ack.to_message()).await?;

    let (tx, rx) = mpsc::channel::<Frame>(shared.config.send_queue);
    let id = shared.registry.add(claims.subject.clone(), tx);
    let _registered = Registered {
        registry: &shared.registry,
        id,
    };
    info!(%peer, subject = %claims.subject, "authenticated");

    let result = session(ws, rx, id, &claims, &shared).await;
    info!(%peer, subject = %claims.subject, "disconnected");
    result
}

async fn authenticate(ws: &mut WebSocketStream<TcpStream>, shared: &Shared) -> Result<Claims> {
    let first = tokio::time::timeout(shared.config.auth_timeout, ws.next())
        .await
        .map_err(|_| Error::Unauthenticated("auth timeout".into()))?;

    let text = match first {
        Some(Ok(Message::Text(t))) => t,
        Some(Ok(Message::Close(_))) | None => {
            return Err(Error::Unauthenticated("closed before auth".into()))
        }
        Some(Ok(_)) => return Err(Error::Unauthenticated("expected an auth message".into())),
        Some(Err(e)) => return Err(e.into()),
    };

    let token = match serde_json::from_str::<ClientMessage>(&text) {
        Ok(ClientMessage::Auth { token }) => token,
        _ => return Err(Error::Unauthenticated("expected an auth message".into())),
    };

    shared
        .validator
        .validate(&token)
        .map_err(Error::Unauthenticated)
}

/// Send one message, giving up if the socket has not taken it within `within`.
///
/// Every write here happens inside a `select!` branch, and a branch body runs to
/// completion before the loop selects again. An unbounded `send` on a peer that has
/// stopped reading would therefore park the heartbeat, the inbound reads and the
/// slow-client check for as long as the peer likes.
async fn send_now<S>(sink: &mut S, msg: Message, within: Duration) -> Result<()>
where
    S: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::time::timeout(within, sink.send(msg))
        .await
        .map_err(|_| Error::Protocol("write timed out".into()))?
        .map_err(Into::into)
}

/// The authenticated lifetime of one connection.
///
/// Liveness is any inbound traffic: a Pong frame answering our Ping frame, a text
/// message, an application-level `ping`. Browsers answer Ping *frames* automatically and
/// never send Pong frames on their own, which is why the server pings with a frame and
/// not with JSON — the original design did the reverse and dropped every browser at the
/// first timeout.
async fn session(
    ws: WebSocketStream<TcpStream>,
    mut rx: mpsc::Receiver<Frame>,
    id: ConnectionId,
    claims: &Claims,
    shared: &Shared,
) -> Result<()> {
    let (mut sink, mut source) = ws.split();
    let mut heartbeat = tokio::time::interval(shared.config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // the first tick is immediate; skip it
    let mut last_seen = tokio::time::Instant::now();
    let write_timeout = shared.config.heartbeat_timeout;

    loop {
        tokio::select! {
            outbound = rx.recv() => {
                let Some(frame) = outbound else { break };
                send_now(&mut sink, frame.to_message(), write_timeout).await?;
            }

            inbound = source.next() => {
                let Some(msg) = inbound else { break };
                last_seen = tokio::time::Instant::now();
                match msg? {
                    Message::Text(text) => {
                        if let Some(reply) = handle(&text, id, claims, shared) {
                            shared.registry.send(id, &reply);
                        }
                    }
                    Message::Ping(payload) => {
                        send_now(&mut sink, Message::Pong(payload), write_timeout).await?
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                    Message::Binary(_) => {
                        let err = ServerMessage::Error {
                            code: "protocol",
                            message: "binary frames are not part of this protocol".into(),
                        }
                        .to_frame();
                        shared.registry.send(id, &err);
                    }
                    Message::Frame(_) => {}
                }
            }

            _ = heartbeat.tick() => {
                if last_seen.elapsed() > shared.config.heartbeat_timeout {
                    warn!(subject = %claims.subject, "heartbeat timeout");
                    shared.registry.metrics().heartbeat_timeouts
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = sink.close().await;
                    break;
                }
                // A Ping *frame*: every conforming client, browsers included, answers it.
                send_now(&mut sink, Message::Ping(Vec::new().into()), write_timeout).await?;
            }
        }

        // Every dropped frame counts, whichever path dropped it — fan-out included, which
        // is the only one a genuinely slow client produces.
        if shared.registry.drops(id) >= shared.config.max_consecutive_drops {
            warn!(subject = %claims.subject, "too slow; closing");
            let _ = sink.close().await;
            break;
        }
    }
    Ok(())
}

/// Handle one client message; the reply, if any, goes back on the connection's queue.
fn handle(text: &str, id: ConnectionId, claims: &Claims, shared: &Shared) -> Option<Frame> {
    let msg = match serde_json::from_str::<ClientMessage>(text) {
        Ok(m) => m,
        Err(e) => {
            return Some(
                ServerMessage::Error {
                    code: "invalid_message",
                    message: e.to_string(),
                }
                .to_frame(),
            )
        }
    };

    match msg {
        ClientMessage::Auth { .. } => Some(
            ServerMessage::Error {
                code: "already_authenticated",
                message: "auth is accepted only as the first message".into(),
            }
            .to_frame(),
        ),

        ClientMessage::Subscribe { channels } => {
            let mut rejected = Vec::new();
            let (sized, oversized): (Vec<String>, Vec<String>) = channels
                .into_iter()
                .partition(|ch| ch.len() <= shared.config.max_channel_len);
            let (allowed, unauthorized): (Vec<String>, Vec<String>) = sized
                .into_iter()
                .partition(|ch| shared.authorizer.may_subscribe(claims, ch));
            let (added, over_limit) =
                shared
                    .registry
                    .subscribe(id, allowed, shared.config.max_subscriptions);

            for ch in &oversized {
                rejected.push(Rejected {
                    channel: ch,
                    reason: "invalid",
                });
            }
            for ch in &unauthorized {
                rejected.push(Rejected {
                    channel: ch,
                    reason: "unauthorized",
                });
            }
            for ch in &over_limit {
                rejected.push(Rejected {
                    channel: ch,
                    reason: "limit",
                });
            }

            let ack = ServerMessage::Subscribed {
                channels: &added,
                rejected,
            }
            .to_frame();
            shared.registry.send(id, &ack);

            // Snapshots follow the acknowledgement, one per channel that has one, so the
            // client can set up its state before the first delta arrives.
            for ch in &added {
                if let Some(payload) = shared.snapshots.snapshot(ch) {
                    let snap = ServerMessage::Snapshot {
                        channel: ch,
                        payload: &payload,
                    }
                    .to_frame();
                    shared.registry.send(id, &snap);
                }
            }
            None
        }

        ClientMessage::Unsubscribe { channels } => {
            let removed = shared.registry.unsubscribe(id, channels);
            Some(ServerMessage::Unsubscribed { channels: &removed }.to_frame())
        }

        ClientMessage::Ping { timestamp } => Some(ServerMessage::Pong { timestamp }.to_frame()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_config_is_valid() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn zero_valued_config_is_refused_rather_than_panicking_later() {
        // `mpsc::channel(0)` and `interval(Duration::ZERO)` both panic inside tokio, well
        // after `bind` has returned and on a task the caller cannot catch.
        for spoil in [
            (|c: &mut Config| c.send_queue = 0) as fn(&mut Config),
            |c| c.heartbeat_interval = Duration::ZERO,
            |c| c.max_connections = 0,
            |c| c.max_message_size = 0,
            |c| c.max_subscriptions = 0,
            |c| c.max_channel_len = 0,
        ] {
            let mut config = Config::default();
            spoil(&mut config);
            assert!(matches!(config.validate(), Err(Error::Config(_))));
        }
    }
}
