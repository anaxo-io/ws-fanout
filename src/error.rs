//! Error type.

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Something went wrong starting or running the server.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The listener could not be bound or accepted a connection badly.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The WebSocket handshake or transport failed.
    #[error("websocket: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),

    /// The client sent something that is not this protocol.
    #[error("protocol: {0}")]
    Protocol(String),

    /// The token did not validate. The message is sent to the client, so keep it short
    /// and free of secrets.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),

    /// The server is at its connection limit.
    #[error("server full: {max} connections")]
    Full {
        /// The configured maximum.
        max: usize,
    },
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::WebSocket(Box::new(e))
    }
}
