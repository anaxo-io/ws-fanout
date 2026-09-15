//! The shared-memory input path: bytes in a queue become frames on a socket.
#![cfg(feature = "outcry")]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use ws_fanout::outcry_source::{Decoded, Idle};
use ws_fanout::{Server, StaticToken};

#[tokio::test(flavor = "multi_thread")]
async fn frames_written_to_the_queue_reach_a_subscriber() {
    let path = std::env::temp_dir().join(format!("ws-fanout-test-{}", std::process::id()));
    let queue = outcry::Queue::create(&path, 1 << 16).unwrap();
    let mut producer = queue.producer().unwrap();
    let consumer = outcry::Queue::open(&path).unwrap().consumer();

    let s = Server::builder()
        .validator(StaticToken::new("t", "tester"))
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let source = s.consume_outcry(consumer, Idle::Sleep(Duration::from_micros(100)), |bytes| {
        let text = std::str::from_utf8(bytes).ok()?;
        let (channel, payload) = text.split_once('\n')?;
        Some(Decoded {
            channel: channel.to_string(),
            payload: serde_json::value::RawValue::from_string(payload.to_string()).ok()?,
        })
    });

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{}", s.local_addr()))
        .await
        .unwrap();
    ws.send(Message::Text(
        json!({"type":"auth","token":"t"}).to_string().into(),
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap(); // authenticated
    ws.send(Message::Text(
        json!({"type":"subscribe","channels":["q"]})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    ws.next().await.unwrap().unwrap(); // subscribed
    while s.subscribers("q") == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    producer.write(b"q\n{\"seq\":1}").unwrap();
    producer.write(b"other\n{\"seq\":2}").unwrap();
    producer.write(b"q\n{\"seq\":3}").unwrap();

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let m = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(t) = m {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            assert_eq!(v["channel"], "q");
            seen.push(v["payload"]["seq"].as_u64().unwrap());
        }
    }
    assert_eq!(seen, [1, 3]);

    drop(s);
    source.join().unwrap();
    let _ = std::fs::remove_file(&path);
}
