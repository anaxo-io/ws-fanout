//! Feed the server from an [`outcry`] shared-memory queue.
//!
//! A dedicated OS thread — never a tokio worker, since `try_read` spins — drains the
//! consumer and hands each frame to a decoder that turns bytes into `(channel, payload)`.
//! The server publishes the result. If the consumer is overrun, the thread resyncs and
//! the server tells every client to discard what it holds: the upstream lost data, so
//! nothing downstream can be trusted until the next snapshot.

use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::value::RawValue;

use crate::Server;

/// A frame from the queue, decoded.
pub struct Decoded {
    /// Channel to publish on.
    pub channel: String,
    /// JSON payload, published verbatim.
    pub payload: Box<RawValue>,
}

/// Where the source thread should spend its time when the queue is empty.
#[derive(Debug, Clone, Copy)]
pub enum Idle {
    /// Spin. Lowest latency; burns a core.
    Spin,
    /// Sleep this long between polls once the queue has been empty for a while.
    Sleep(Duration),
}

impl Server {
    /// Start a thread that drains `consumer` into this server.
    ///
    /// `decode` receives each frame's bytes and returns what to publish, or `None` to skip
    /// it. Frames the decoder rejects are counted and otherwise ignored.
    ///
    /// The thread ends when the server is dropped. The returned handle can be joined; it
    /// never panics on queue errors, which are logged.
    pub fn consume_outcry<F>(
        &self,
        mut consumer: outcry::Consumer,
        idle: Idle,
        mut decode: F,
    ) -> JoinHandle<()>
    where
        F: FnMut(&[u8]) -> Option<Decoded> + Send + 'static,
    {
        let server = self.clone();
        let alive = Arc::downgrade(&self.alive);
        std::thread::Builder::new()
            .name("ws-fanout-outcry".into())
            .spawn(move || {
                let mut buf = vec![0u8; 64 * 1024];
                let mut idle_polls: u32 = 0;
                let mut undecodable: u64 = 0;

                // `server` is our own clone; the caller's clones are what `alive` counts.
                while alive.strong_count() > 1 {
                    match consumer.try_read(&mut buf) {
                        Ok(Some(n)) => {
                            idle_polls = 0;
                            match decode(&buf[..n]) {
                                Some(d) => {
                                    server.publish(&d.channel, &d.payload);
                                }
                                None => {
                                    undecodable += 1;
                                    if undecodable.is_power_of_two() {
                                        tracing::warn!(undecodable, "frames the decoder rejected");
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            idle_polls = idle_polls.saturating_add(1);
                            match idle {
                                Idle::Spin => std::hint::spin_loop(),
                                Idle::Sleep(d) => {
                                    if idle_polls > 1_000 {
                                        std::thread::sleep(d);
                                    } else {
                                        std::hint::spin_loop();
                                    }
                                }
                            }
                        }
                        Err(outcry::Error::Overrun { behind }) => {
                            let skipped = consumer.resync();
                            tracing::warn!(behind, skipped, "overrun; resynced, clients told");
                            server.resync_all();
                        }
                        Err(outcry::Error::BufferTooSmall { needed, .. }) => {
                            buf.resize(needed, 0);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "outcry consumer failed; source stopping");
                            break;
                        }
                    }
                }
            })
            .expect("spawning the outcry source thread")
    }
}
