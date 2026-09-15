//! End-to-end behaviour over real sockets.

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use ws_fanout::{Claims, LastValue, Server, StaticToken};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

const TOKEN: &str = "test-token";

async fn server() -> Server {
    Server::builder()
        .validator(StaticToken::new(TOKEN, "tester"))
        .bind("127.0.0.1:0")
        .await
        .unwrap()
}

async fn connect(server: &Server) -> Ws {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}", server.local_addr()))
        .await
        .unwrap();
    ws
}

async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}

/// Next JSON text message, skipping control frames. Panics after two seconds.
async fn recv(ws: &mut Ws) -> Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match ws.next().await.expect("connection closed").unwrap() {
                Message::Text(t) => return serde_json::from_str(&t).unwrap(),
                Message::Ping(_) | Message::Pong(_) => continue,
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await
    .expect("timed out waiting for a message")
}

async fn authed(server: &Server) -> Ws {
    let mut ws = connect(server).await;
    send(&mut ws, json!({"type": "auth", "token": TOKEN})).await;
    let ack = recv(&mut ws).await;
    assert_eq!(ack["type"], "authenticated");
    assert_eq!(ack["subject"], "tester");
    ws
}

async fn subscribe(ws: &mut Ws, channels: &[&str]) -> Value {
    send(ws, json!({"type": "subscribe", "channels": channels})).await;
    let ack = recv(ws).await;
    assert_eq!(ack["type"], "subscribed");
    ack
}

/// Poll until `f` is true or two seconds pass.
async fn eventually(mut f: impl FnMut() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not met in time");
}

#[tokio::test]
async fn auth_is_acknowledged_before_anything_else() {
    let s = server().await;
    let _ws = authed(&s).await;
    eventually(|| s.connections() == 1).await;
}

#[tokio::test]
async fn bad_token_is_refused_with_an_error() {
    let s = server().await;
    let mut ws = connect(&s).await;
    send(&mut ws, json!({"type": "auth", "token": "wrong"})).await;
    let err = recv(&mut ws).await;
    assert_eq!(err["type"], "error");
    assert_eq!(err["code"], "unauthenticated");
    eventually(|| s.connections() == 0).await;
}

#[tokio::test]
async fn first_message_must_be_auth() {
    let s = server().await;
    let mut ws = connect(&s).await;
    send(&mut ws, json!({"type": "subscribe", "channels": ["x"]})).await;
    let err = recv(&mut ws).await;
    assert_eq!(err["code"], "unauthenticated");
}

#[tokio::test]
async fn subscribe_then_publish_fans_out_to_every_subscriber_once() {
    let s = server().await;
    let mut a = authed(&s).await;
    let mut b = authed(&s).await;
    let mut c = authed(&s).await;
    let ack = subscribe(&mut a, &["ticker:BTC"]).await;
    assert_eq!(ack["channels"], json!(["ticker:BTC"]));
    assert!(
        ack.get("rejected").is_none(),
        "rejected is omitted when empty"
    );
    subscribe(&mut b, &["ticker:BTC", "ticker:ETH"]).await;
    subscribe(&mut c, &["ticker:ETH"]).await;
    eventually(|| s.subscribers("ticker:BTC") == 2).await;

    let n = s.publish_json("ticker:BTC", &json!({"last": 1.5}));
    assert_eq!(n, 2);

    for ws in [&mut a, &mut b] {
        let m = recv(ws).await;
        assert_eq!(m["type"], "data");
        assert_eq!(m["channel"], "ticker:BTC");
        assert_eq!(m["payload"]["last"], 1.5);
    }
    // c is not subscribed to BTC: a ping round-trip proves nothing else arrived first.
    send(&mut c, json!({"type": "ping", "timestamp": 7})).await;
    let m = recv(&mut c).await;
    assert_eq!(m["type"], "pong");
    assert_eq!(m["timestamp"], 7);
}

#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let s = server().await;
    let mut ws = authed(&s).await;
    subscribe(&mut ws, &["ch"]).await;
    send(&mut ws, json!({"type": "unsubscribe", "channels": ["ch"]})).await;
    let ack = recv(&mut ws).await;
    assert_eq!(ack["type"], "unsubscribed");
    assert_eq!(ack["channels"], json!(["ch"]));
    eventually(|| s.subscribers("ch") == 0).await;
    assert_eq!(s.publish_json("ch", &json!(1)), 0);
}

#[tokio::test]
async fn payload_is_embedded_verbatim() {
    let s = server().await;
    let mut ws = authed(&s).await;
    subscribe(&mut ws, &["raw"]).await;
    eventually(|| s.subscribers("raw") == 1).await;
    let raw = serde_json::value::RawValue::from_string(r#"{"a":[1,2,{"b":null}]}"#.into()).unwrap();
    s.publish("raw", &raw);
    let m = recv(&mut ws).await;
    assert_eq!(m["payload"], json!({"a": [1, 2, {"b": null}]}));
}

#[tokio::test]
async fn snapshot_follows_the_subscribe_ack() {
    let snaps = Arc::new(LastValue::new());
    snaps.record(
        "book",
        &serde_json::value::to_raw_value(&json!({"bids": [[1, 2]]})).unwrap(),
    );
    struct Src(Arc<LastValue>);
    impl ws_fanout::SnapshotSource for Src {
        fn snapshot(&self, ch: &str) -> Option<Box<serde_json::value::RawValue>> {
            self.0.snapshot(ch)
        }
    }
    let s = Server::builder()
        .validator(StaticToken::new(TOKEN, "tester"))
        .snapshots(Src(snaps))
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let mut ws = authed(&s).await;
    subscribe(&mut ws, &["book", "nothing"]).await;
    let m = recv(&mut ws).await;
    assert_eq!(m["type"], "snapshot");
    assert_eq!(m["channel"], "book");
    assert_eq!(m["payload"]["bids"], json!([[1, 2]]));
}

#[tokio::test]
async fn authorizer_rejects_per_channel() {
    let s = Server::builder()
        .validator(StaticToken::new(TOKEN, "tester"))
        .authorizer(|_: &Claims, ch: &str| ch.starts_with("public:"))
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let mut ws = authed(&s).await;
    let ack = subscribe(&mut ws, &["public:a", "private:b"]).await;
    assert_eq!(ack["channels"], json!(["public:a"]));
    assert_eq!(
        ack["rejected"],
        json!([{"channel": "private:b", "reason": "unauthorized"}])
    );
}

#[tokio::test]
async fn subscription_limit_is_reported() {
    let cfg = ws_fanout::Config {
        max_subscriptions: 1,
        ..Default::default()
    };
    let s = Server::builder()
        .config(cfg)
        .validator(StaticToken::new(TOKEN, "tester"))
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let mut ws = authed(&s).await;
    let ack = subscribe(&mut ws, &["a", "b"]).await;
    assert_eq!(ack["channels"], json!(["a"]));
    assert_eq!(ack["rejected"][0]["reason"], "limit");
}

#[tokio::test]
async fn max_connections_refuses_the_extra_one() {
    let s = Server::builder()
        .validator(StaticToken::new(TOKEN, "tester"))
        .max_connections(1)
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let _a = authed(&s).await;
    eventually(|| s.connections() == 1).await;
    let refused = tokio_tungstenite::connect_async(format!("ws://{}", s.local_addr())).await;
    assert!(
        refused.is_err(),
        "second connection should be refused before the handshake"
    );
    eventually(|| {
        s.metrics()
            .refused_full
            .load(std::sync::atomic::Ordering::Relaxed)
            == 1
    })
    .await;
}

#[tokio::test]
async fn resync_all_reaches_every_connection() {
    let s = server().await;
    let mut a = authed(&s).await;
    let mut b = authed(&s).await;
    eventually(|| s.connections() == 2).await;
    assert_eq!(s.resync_all(), 2);
    for ws in [&mut a, &mut b] {
        assert_eq!(recv(ws).await["type"], "resync");
    }
}

#[tokio::test]
async fn invalid_json_gets_an_error_not_a_disconnect() {
    let s = server().await;
    let mut ws = authed(&s).await;
    ws.send(Message::Text("{not json".into())).await.unwrap();
    let err = recv(&mut ws).await;
    assert_eq!(err["code"], "invalid_message");
    send(&mut ws, json!({"type": "ping", "timestamp": 1})).await;
    assert_eq!(recv(&mut ws).await["type"], "pong");
}

#[tokio::test]
async fn oversized_message_closes_the_connection() {
    let cfg = ws_fanout::Config {
        max_message_size: 256,
        ..Default::default()
    };
    let s = Server::builder()
        .config(cfg)
        .validator(StaticToken::new(TOKEN, "tester"))
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let mut ws = authed(&s).await;
    let big = json!({"type": "subscribe", "channels": ["x".repeat(1000)]});
    let _ = send(&mut ws, big).await;
    eventually(|| s.connections() == 0).await;
}

// ---- heartbeat ------------------------------------------------------------------------

fn fast_heartbeat() -> ws_fanout::Builder {
    Server::builder()
        .validator(StaticToken::new(TOKEN, "tester"))
        .heartbeat(Duration::from_millis(50), Duration::from_millis(120))
}

/// A browser: answers Ping frames (tungstenite does this automatically on read) and never
/// sends anything on its own. It must survive.
#[tokio::test]
async fn browser_like_client_answering_ping_frames_survives() {
    let s = fast_heartbeat().bind("127.0.0.1:0").await.unwrap();
    let mut ws = authed(&s).await;
    eventually(|| s.connections() == 1).await;
    // Keep reading so tungstenite answers the pings; nothing else for 8 intervals.
    let read = async {
        while let Some(Ok(m)) = ws.next().await {
            assert!(matches!(m, Message::Ping(_)), "unexpected {m:?}");
        }
    };
    let _ = tokio::time::timeout(Duration::from_millis(400), read).await;
    assert_eq!(s.connections(), 1, "a client answering pings was dropped");
    assert_eq!(
        s.metrics()
            .heartbeat_timeouts
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// A client that never reads and never writes is gone.
#[tokio::test]
async fn silent_client_is_dropped_after_the_timeout() {
    let s = fast_heartbeat().bind("127.0.0.1:0").await.unwrap();
    let ws = authed(&s).await;
    eventually(|| s.connections() == 1).await;
    // Hold the socket without reading: no Pong frames can go back.
    let held = ws;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(s.connections(), 0, "a silent client was kept");
    assert_eq!(
        s.metrics()
            .heartbeat_timeouts
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    drop(held);
}

/// A client whose only traffic is application-level JSON pings also survives.
#[tokio::test]
async fn json_ping_counts_as_liveness() {
    let s = fast_heartbeat().bind("127.0.0.1:0").await.unwrap();
    let mut ws = authed(&s).await;
    for i in 0..8 {
        send(&mut ws, json!({"type": "ping", "timestamp": i})).await;
        assert_eq!(recv(&mut ws).await["type"], "pong");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(s.connections(), 1);
}

// ---- slow consumer ----------------------------------------------------------------------

#[tokio::test]
async fn slow_client_drops_frames_and_publisher_never_blocks() {
    let cfg = ws_fanout::Config {
        send_queue: 4,
        max_consecutive_drops: u64::MAX,
        ..Default::default()
    };
    let s = Server::builder()
        .config(cfg)
        .validator(StaticToken::new(TOKEN, "tester"))
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let mut ws = authed(&s).await;
    subscribe(&mut ws, &["fast"]).await;
    eventually(|| s.subscribers("fast") == 1).await;
    // The client is not reading. Publish far more than the queue plus socket buffers hold.
    let payload = "x".repeat(16 * 1024);
    let started = std::time::Instant::now();
    for i in 0..2_000 {
        s.publish_json("fast", &json!({"i": i, "pad": payload}));
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "publish blocked"
    );
    let dropped = s
        .metrics()
        .frames_dropped
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        dropped > 0,
        "expected drops with a 4-deep queue and 32 MiB unread"
    );
}

#[cfg(feature = "jwt")]
#[tokio::test]
async fn hs256_jwt_is_accepted_and_subject_is_the_claim() {
    use jsonwebtoken::{encode, EncodingKey, Header};
    let secret = b"a-test-secret-that-is-long-enough";
    let s = Server::builder()
        .validator(ws_fanout::HmacJwt::new(secret))
        .bind("127.0.0.1:0")
        .await
        .unwrap();
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 60;
    let token = encode(
        &Header::default(),
        &json!({"sub": "alice", "exp": exp, "tier": "pro"}),
        &EncodingKey::from_secret(secret),
    )
    .unwrap();
    let mut ws = connect(&s).await;
    send(&mut ws, json!({"type": "auth", "token": token})).await;
    let ack = recv(&mut ws).await;
    assert_eq!(ack["type"], "authenticated");
    assert_eq!(ack["subject"], "alice");
}
