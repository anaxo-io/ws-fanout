//! Connections and their subscriptions.
//!
//! The registry is synchronous on purpose: [`Registry::publish`] is called from whatever
//! thread produces data — a tokio task, or a dedicated thread draining a shared-memory
//! queue — and never awaits. Delivery to each connection is a non-blocking `try_send` into
//! that connection's bounded queue; a full queue drops the frame and counts it. The slow
//! client pays, never the publisher and never the other clients.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;

use crate::protocol::Frame;

/// Opaque connection identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

struct Connection {
    subject: String,
    subscriptions: HashSet<String>,
    tx: mpsc::Sender<Frame>,
    dropped: u64,
}

/// Counters, all monotonic except `connections_active`.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Connections currently open.
    pub connections_active: AtomicU64,
    /// Connections opened since start.
    pub connections_total: AtomicU64,
    /// Connections closed because the client stopped answering heartbeats.
    pub heartbeat_timeouts: AtomicU64,
    /// Connections refused because the server was full.
    pub refused_full: AtomicU64,
    /// Frames handed to a connection's queue.
    pub frames_sent: AtomicU64,
    /// Frames dropped because a connection's queue was full.
    pub frames_dropped: AtomicU64,
}

impl Metrics {
    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

/// All connections, keyed by id, with per-channel subscriber lookup.
pub struct Registry {
    inner: RwLock<Inner>,
    next_id: AtomicU64,
    metrics: Arc<Metrics>,
}

#[derive(Default)]
struct Inner {
    connections: HashMap<ConnectionId, Connection>,
    /// channel → subscribed connections. Kept in step with each connection's set so
    /// publish is a single map lookup rather than a scan of every connection.
    by_channel: HashMap<String, HashSet<ConnectionId>>,
}

impl Registry {
    pub(crate) fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            inner: RwLock::new(Inner::default()),
            next_id: AtomicU64::new(1),
            metrics,
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Inner> {
        self.inner.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Inner> {
        self.inner.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Counters.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub(crate) fn add(&self, subject: String, tx: mpsc::Sender<Frame>) -> ConnectionId {
        let id = ConnectionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.write().connections.insert(
            id,
            Connection {
                subject,
                subscriptions: HashSet::new(),
                tx,
                dropped: 0,
            },
        );
        Metrics::add(&self.metrics.connections_total, 1);
        Metrics::add(&self.metrics.connections_active, 1);
        id
    }

    pub(crate) fn remove(&self, id: ConnectionId) {
        let mut inner = self.write();
        if let Some(conn) = inner.connections.remove(&id) {
            for ch in conn.subscriptions {
                if let Some(set) = inner.by_channel.get_mut(&ch) {
                    set.remove(&id);
                    if set.is_empty() {
                        inner.by_channel.remove(&ch);
                    }
                }
            }
            self.metrics
                .connections_active
                .fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Number of open connections.
    pub fn len(&self) -> usize {
        self.read().connections.len()
    }

    /// Whether no connections are open.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of subscribers to `channel`.
    pub fn subscribers(&self, channel: &str) -> usize {
        self.read().by_channel.get(channel).map_or(0, HashSet::len)
    }

    /// Add subscriptions, respecting `max` per connection. Returns the channels that were
    /// newly added and the ones refused for the limit.
    pub(crate) fn subscribe(
        &self,
        id: ConnectionId,
        channels: impl IntoIterator<Item = String>,
        max: usize,
    ) -> (Vec<String>, Vec<String>) {
        let mut inner = self.write();
        let mut added = Vec::new();
        let mut over_limit = Vec::new();

        let Some(conn) = inner.connections.get_mut(&id) else {
            return (added, over_limit);
        };
        for ch in channels {
            if conn.subscriptions.contains(&ch) {
                continue; // already subscribed: idempotent, not an error
            }
            if conn.subscriptions.len() >= max {
                over_limit.push(ch);
                continue;
            }
            conn.subscriptions.insert(ch.clone());
            added.push(ch);
        }
        for ch in &added {
            inner.by_channel.entry(ch.clone()).or_default().insert(id);
        }
        (added, over_limit)
    }

    pub(crate) fn unsubscribe(
        &self,
        id: ConnectionId,
        channels: impl IntoIterator<Item = String>,
    ) -> Vec<String> {
        let mut inner = self.write();
        let mut removed = Vec::new();
        let Some(conn) = inner.connections.get_mut(&id) else {
            return removed;
        };
        for ch in channels {
            if conn.subscriptions.remove(&ch) {
                removed.push(ch);
            }
        }
        for ch in &removed {
            if let Some(set) = inner.by_channel.get_mut(ch) {
                set.remove(&id);
                if set.is_empty() {
                    inner.by_channel.remove(ch);
                }
            }
        }
        removed
    }

    /// Deliver `frame` to every subscriber of `channel`. Returns how many received it.
    pub fn publish(&self, channel: &str, frame: &Frame) -> usize {
        let inner = self.read();
        let Some(ids) = inner.by_channel.get(channel) else {
            return 0;
        };
        let mut delivered = 0;
        for id in ids {
            if let Some(conn) = inner.connections.get(id) {
                if self.deliver(conn, frame) {
                    delivered += 1;
                }
            }
        }
        delivered
    }

    /// Deliver `frame` to every connection, subscribed or not.
    pub fn broadcast(&self, frame: &Frame) -> usize {
        let inner = self.read();
        inner
            .connections
            .values()
            .filter(|c| self.deliver(c, frame))
            .count()
    }

    /// Deliver to one connection.
    pub(crate) fn send(&self, id: ConnectionId, frame: &Frame) -> bool {
        let inner = self.read();
        inner
            .connections
            .get(&id)
            .is_some_and(|c| self.deliver(c, frame))
    }

    fn deliver(&self, conn: &Connection, frame: &Frame) -> bool {
        match conn.tx.try_send(frame.clone()) {
            Ok(()) => {
                Metrics::add(&self.metrics.frames_sent, 1);
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                Metrics::add(&self.metrics.frames_dropped, 1);
                tracing::debug!(subject = %conn.subject, "queue full; frame dropped");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// How many frames this connection has dropped. Used by the handler to decide when a
    /// client is too slow to keep.
    pub(crate) fn record_drop(&self, id: ConnectionId) -> u64 {
        let mut inner = self.write();
        match inner.connections.get_mut(&id) {
            Some(c) => {
                c.dropped += 1;
                c.dropped
            }
            None => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(s: &str) -> Frame {
        Frame(s.into())
    }

    #[test]
    fn publish_reaches_subscribers_only() {
        let reg = Registry::new(Arc::new(Metrics::default()));
        let (tx1, mut rx1) = mpsc::channel(8);
        let (tx2, mut rx2) = mpsc::channel(8);
        let a = reg.add("a".into(), tx1);
        let _b = reg.add("b".into(), tx2);

        reg.subscribe(a, ["ch".to_string()], 10);
        assert_eq!(reg.subscribers("ch"), 1);
        assert_eq!(reg.publish("ch", &frame("x")), 1);
        assert_eq!(rx1.try_recv().unwrap().as_str(), "x");
        assert!(rx2.try_recv().is_err());
        assert_eq!(reg.publish("other", &frame("y")), 0);
    }

    #[test]
    fn subscription_limit_and_idempotence() {
        let reg = Registry::new(Arc::new(Metrics::default()));
        let (tx, _rx) = mpsc::channel(8);
        let id = reg.add("a".into(), tx);

        let (added, over) = reg.subscribe(id, ["a".into(), "b".into(), "c".into()], 2);
        assert_eq!(added, ["a", "b"]);
        assert_eq!(over, ["c"]);

        let (added, over) = reg.subscribe(id, ["a".into()], 2);
        assert!(
            added.is_empty() && over.is_empty(),
            "re-subscribing is a no-op"
        );

        assert_eq!(reg.unsubscribe(id, ["a".into(), "zzz".into()]), ["a"]);
        assert_eq!(reg.subscribers("a"), 0);
    }

    #[test]
    fn remove_cleans_channel_index() {
        let reg = Registry::new(Arc::new(Metrics::default()));
        let (tx, _rx) = mpsc::channel(8);
        let id = reg.add("a".into(), tx);
        reg.subscribe(id, ["ch".into()], 10);
        reg.remove(id);
        assert_eq!(reg.subscribers("ch"), 0);
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.metrics().connections_active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn full_queue_drops_and_counts() {
        let reg = Registry::new(Arc::new(Metrics::default()));
        let (tx, _rx) = mpsc::channel(1);
        let id = reg.add("slow".into(), tx);
        reg.subscribe(id, ["ch".into()], 10);
        assert_eq!(reg.publish("ch", &frame("1")), 1);
        assert_eq!(reg.publish("ch", &frame("2")), 0, "queue of one is full");
        assert_eq!(reg.metrics().frames_dropped.load(Ordering::Relaxed), 1);
    }
}
