//! A gateway fed from an outcry queue in shared memory.
//!
//! ```bash
//! cargo run --features outcry --example gateway          # this
//! cargo run --features outcry --example synthetic_feed   # something writing the queue
//! ```
//!
//! Then, from a browser console on any page:
//!
//! ```js
//! const ws = new WebSocket("ws://127.0.0.1:9001");
//! ws.onopen = () => ws.send(JSON.stringify({type:"auth", token:"demo"}));
//! ws.onmessage = e => { const m = JSON.parse(e.data); console.log(m);
//!   if (m.type === "authenticated") ws.send(JSON.stringify({type:"subscribe", channels:["ticker:BTC-USD"]})); };
//! ```
//!
//! Frames on the queue are `channel\n{json}`: the channel name, a newline, the payload.

use std::sync::Arc;
use std::time::Duration;

use serde_json::value::RawValue;
use ws_fanout::outcry_source::{Decoded, Idle};
use ws_fanout::{LastValue, Server, StaticToken};

const QUEUE: &str = "/dev/shm/ws-fanout-demo";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = outcry::Queue::open(QUEUE)
        .map_err(|e| format!("open {QUEUE}: {e} — is synthetic_feed running?"))?;
    let consumer = queue.consumer();

    // Remember the last payload per channel so a new subscriber gets state immediately.
    let snapshots = Arc::new(LastValue::new());

    let server = Server::builder()
        .validator(StaticToken::new("demo", "demo-user"))
        .snapshots(SharedLastValue(Arc::clone(&snapshots)))
        .heartbeat(Duration::from_secs(15), Duration::from_secs(45))
        .bind("127.0.0.1:9001")
        .await?;
    println!("gateway listening on ws://{}", server.local_addr());

    let source = server.consume_outcry(
        consumer,
        Idle::Sleep(Duration::from_micros(200)),
        move |bytes| {
            let text = std::str::from_utf8(bytes).ok()?;
            let (channel, json) = text.split_once('\n')?;
            let payload = RawValue::from_string(json.to_string()).ok()?;
            snapshots.record(channel, &payload);
            Some(Decoded {
                channel: channel.to_string(),
                payload,
            })
        },
    );

    tokio::signal::ctrl_c().await?;
    println!(
        "stopping; {} connections, {} frames sent, {} dropped",
        server.connections(),
        server
            .metrics()
            .frames_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        server
            .metrics()
            .frames_dropped
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    drop(server);
    let _ = source.join();
    Ok(())
}

/// `LastValue` behind an `Arc`, so the decoder closure and the server can share it.
struct SharedLastValue(Arc<LastValue>);

impl ws_fanout::SnapshotSource for SharedLastValue {
    fn snapshot(&self, channel: &str) -> Option<Box<RawValue>> {
        self.0.snapshot(channel)
    }
}
