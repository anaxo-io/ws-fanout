//! Write a synthetic ticker into the demo queue, ten times a second.
//!
//! ```bash
//! cargo run --features outcry --example synthetic_feed
//! ```
//!
//! Frames are `channel\n{json}`, the encoding `examples/gateway.rs` decodes. A real feed
//! — an exchange connector, say — would write the same shape from its own process.

use std::time::{Duration, Instant};

const QUEUE: &str = "/dev/shm/ws-fanout-demo";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let queue = outcry::Queue::create(QUEUE, 1 << 20)?;
    let mut producer = queue.producer()?;
    println!("writing ticker:BTC-USD to {QUEUE}; Ctrl-C to stop");

    let start = Instant::now();
    let mut seq = 0u64;
    loop {
        let t = start.elapsed().as_secs_f64();
        let last = 79_000.0 + 200.0 * (t / 7.0).sin() + (seq % 7) as f64 * 0.01;
        let frame = format!(
            "ticker:BTC-USD\n{{\"seq\":{seq},\"last\":{last:.2},\"ts_ms\":{}}}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis()
        );
        producer.write(frame.as_bytes())?;
        seq += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
}
