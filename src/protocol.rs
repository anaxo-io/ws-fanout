//! The wire protocol: JSON text frames, tagged by `type`.
//!
//! Channels are opaque strings chosen by the application — `order_book:binance:BTC-USDT`
//! is a convention, not something the server understands. Payloads are opaque JSON: the
//! server embeds whatever the publisher hands it, verbatim, and never re-parses it.
//! Every subscriber to a channel receives byte-identical frames, serialised once.
//!
//! The full exchange is documented in `docs/protocol.md`.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio_tungstenite::tungstenite::protocol::frame::Utf8Bytes;
use tokio_tungstenite::tungstenite::Message;

/// What a client may send.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// The first message on a connection: present a token.
    Auth {
        /// Opaque token, interpreted by the server's [`TokenValidator`](crate::TokenValidator).
        token: String,
    },
    /// Subscribe to channels. Unknown or unauthorised channels are reported, not fatal.
    Subscribe {
        /// Channel names.
        channels: Vec<String>,
    },
    /// Stop receiving these channels.
    Unsubscribe {
        /// Channel names.
        channels: Vec<String>,
    },
    /// Application-level ping. Answered with `pong`, and counts as liveness.
    Ping {
        /// Echoed back unchanged; zero if absent.
        #[serde(default)]
        timestamp: u64,
    },
}

/// What the server sends. Built by [`ServerMessage::to_frame`] into a shared text frame.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage<'a> {
    /// The token was accepted. Sent before anything else; subscriptions are accepted only
    /// after this.
    Authenticated {
        /// Who the token identified.
        subject: &'a str,
    },
    /// Which of the requested channels are now active.
    Subscribed {
        /// Channels the client now receives.
        channels: &'a [String],
        /// Channels it asked for and was refused, with a reason each.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        rejected: Vec<Rejected<'a>>,
    },
    /// Which channels were removed.
    Unsubscribed {
        /// Channels no longer received.
        channels: &'a [String],
    },
    /// A published message.
    Data {
        /// The channel it was published on.
        channel: &'a str,
        /// The publisher's JSON, verbatim.
        payload: &'a RawValue,
    },
    /// A snapshot of a channel's current state, sent immediately after `subscribed` when
    /// the server has a [`SnapshotSource`](crate::SnapshotSource) for it.
    Snapshot {
        /// The channel.
        channel: &'a str,
        /// The snapshot JSON, verbatim.
        payload: &'a RawValue,
    },
    /// The server's upstream lost data. Anything held for these channels should be
    /// discarded; a fresh snapshot follows where one exists.
    Resync {
        /// Affected channels; empty means every channel.
        #[serde(skip_serializing_if = "Vec::is_empty")]
        channels: Vec<&'a str>,
    },
    /// Reply to a client `ping`.
    Pong {
        /// The client's timestamp, echoed.
        timestamp: u64,
    },
    /// Something the client sent was wrong. The connection stays open unless the error
    /// was fatal, in which case a close frame follows.
    Error {
        /// Short machine-readable code.
        code: &'static str,
        /// Human-readable detail.
        message: String,
    },
}

/// A channel the client asked for and did not get.
#[derive(Debug, Clone, Serialize)]
pub struct Rejected<'a> {
    /// The channel.
    pub channel: &'a str,
    /// Why: `unauthorized`, `limit`, or `invalid`.
    pub reason: &'static str,
}

impl ServerMessage<'_> {
    /// Serialise once into a frame that can be sent to any number of connections.
    pub fn to_frame(&self) -> Frame {
        // Every variant is serialisable by construction; an allocation failure is the
        // only way this fails, and that is not something to handle here.
        // `String` converts into the frame's backing buffer without copying; `&str`
        // would not, and that copy would then happen once per subscriber.
        Frame(
            serde_json::to_string(self)
                .expect("ServerMessage is always serialisable")
                .into(),
        )
    }
}

/// A serialised server message, cheap to clone and share between connections.
///
/// The field is private: it holds the same buffer type the WebSocket sink takes, so
/// sending one to a subscriber is a refcount bump rather than a copy of the JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame(Utf8Bytes);

impl Frame {
    /// The JSON text.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The frame as a text message, sharing this frame's buffer.
    pub(crate) fn to_message(&self) -> Message {
        Message::Text(self.0.clone())
    }
}

#[cfg(test)]
impl From<&str> for Frame {
    fn from(s: &str) -> Self {
        Frame(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_messages_parse() {
        let m: ClientMessage = serde_json::from_str(r#"{"type":"auth","token":"t"}"#).unwrap();
        assert!(matches!(m, ClientMessage::Auth { token } if token == "t"));

        let m: ClientMessage =
            serde_json::from_str(r#"{"type":"subscribe","channels":["a","b"]}"#).unwrap();
        assert!(matches!(m, ClientMessage::Subscribe { channels } if channels == ["a", "b"]));

        let m: ClientMessage = serde_json::from_str(r#"{"type":"ping"}"#).unwrap();
        assert!(matches!(m, ClientMessage::Ping { timestamp: 0 }));

        assert!(serde_json::from_str::<ClientMessage>(r#"{"type":"nope"}"#).is_err());
    }

    #[test]
    fn data_frame_embeds_payload_verbatim() {
        let payload = RawValue::from_string(r#"{"bid":1,"ask":2}"#.to_string()).unwrap();
        let frame = ServerMessage::Data {
            channel: "ticker:x",
            payload: &payload,
        }
        .to_frame();
        assert_eq!(
            frame.as_str(),
            r#"{"type":"data","channel":"ticker:x","payload":{"bid":1,"ask":2}}"#
        );
    }

    #[test]
    fn subscribed_omits_empty_rejections() {
        let frame = ServerMessage::Subscribed {
            channels: &["a".to_string()],
            rejected: vec![],
        }
        .to_frame();
        assert_eq!(frame.as_str(), r#"{"type":"subscribed","channels":["a"]}"#);

        let frame = ServerMessage::Subscribed {
            channels: &[],
            rejected: vec![Rejected {
                channel: "b",
                reason: "unauthorized",
            }],
        }
        .to_frame();
        assert!(frame
            .as_str()
            .contains(r#""rejected":[{"channel":"b","reason":"unauthorized"}]"#));
    }

    #[test]
    fn a_frame_is_sent_without_copying_its_json() {
        let frame = ServerMessage::Pong { timestamp: 7 }.to_frame();
        let Message::Text(text) = frame.to_message() else {
            panic!("a frame is always a text message")
        };
        assert_eq!(
            frame.as_str().as_ptr(),
            text.as_str().as_ptr(),
            "the message must share the frame's buffer; one copy per subscriber is the \
             cost this type exists to avoid"
        );
    }

    #[test]
    fn resync_omits_empty_channel_list() {
        assert_eq!(
            ServerMessage::Resync { channels: vec![] }
                .to_frame()
                .as_str(),
            r#"{"type":"resync"}"#
        );
    }
}
