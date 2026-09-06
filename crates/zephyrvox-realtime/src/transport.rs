use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{
    Connector, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{Message, client::IntoClientRequest},
};
use url::Url;

use crate::{AccessToken, BoxFuture, RealtimeError, tls::pinned_client_config};
use zephyrvox_wire::{AUTHORIZATION, ServerCard, TransportScheme};

/// A transport message presented to the connection coordinator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketMessage {
    /// UTF-8 application text.
    Text(String),
    /// Peer ping payload; the coordinator must answer with a pong.
    Ping(Vec<u8>),
    /// Peer pong payload.
    Pong(Vec<u8>),
    /// Close frame and optional UTF-8 reason.
    Close(Option<String>),
}

/// One connected WebSocket session.
///
/// The methods are intentionally object-safe and use boxed futures so tests
/// can supply an in-memory fake without depending on tungstenite. A session
/// has one logical owner: the connection coordinator performs all sends and
/// receives on its Tokio task, avoiding concurrent WebSocket writers.
pub trait WebSocketSession: Send {
    /// Sends one application text frame.
    fn send_text<'a>(&'a mut self, text: String) -> BoxFuture<'a, Result<(), RealtimeError>>;

    /// Sends a protocol pong in response to a peer ping.
    fn send_pong<'a>(&'a mut self, payload: Vec<u8>) -> BoxFuture<'a, Result<(), RealtimeError>>;

    /// Receives the next transport message, or `None` after an orderly EOF.
    fn receive<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<WebSocketMessage>, RealtimeError>>;

    /// Requests an orderly close and releases the underlying socket.
    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), RealtimeError>>;
}

/// Factory for authenticated WebSocket sessions.
///
/// Implementations must use the supplied access token only in the upgrade
/// request. The token is never placed in the URL, and a refresh token is not
/// accepted by this trait. The returned future owns its result and may outlive
/// the caller's stack frame.
pub trait WebSocketConnector: Send + Sync {
    /// Opens one authenticated session for `url`.
    fn connect(
        &self,
        url: Url,
        access_token: AccessToken,
    ) -> BoxFuture<'static, Result<Box<dyn WebSocketSession>, RealtimeError>>;
}

/// The production Tokio/tungstenite connector selected by a [`ServerCard`].
#[derive(Clone)]
pub struct TokioTungsteniteConnector {
    card: ServerCard,
}

impl TokioTungsteniteConnector {
    /// Creates a connector whose TLS mode and SPKI fingerprint are fixed by
    /// `card`. The connector never downgrades a TLS card to plaintext.
    pub fn new(card: ServerCard) -> Self {
        Self { card }
    }
}

impl WebSocketConnector for TokioTungsteniteConnector {
    fn connect(
        &self,
        url: Url,
        access_token: AccessToken,
    ) -> BoxFuture<'static, Result<Box<dyn WebSocketSession>, RealtimeError>> {
        let card = self.card.clone();
        Box::pin(async move {
            let expected_scheme = match card.scheme() {
                TransportScheme::Plain => "ws",
                TransportScheme::Tls => "wss",
            };
            if url.scheme() != expected_scheme {
                return Err(RealtimeError::Protocol(format!(
                    "WebSocket URL scheme {} does not match server card",
                    url.scheme()
                )));
            }
            let mut request = url
                .as_str()
                .into_client_request()
                .map_err(|error| RealtimeError::Transport(error.to_string()))?;
            let value = format!("Bearer {}", access_token.as_str());
            let header = value.parse().map_err(|error| {
                RealtimeError::Transport(format!("invalid authorization header: {error}"))
            })?;
            request.headers_mut().insert(AUTHORIZATION, header);

            let mut pin_failure = None;
            let connector = if let Some(fingerprint) = card.fingerprint() {
                let (config, failure) = pinned_client_config(fingerprint)?;
                pin_failure = Some(failure);
                Some(Connector::Rustls(Arc::new(config)))
            } else {
                None
            };
            let (stream, _) =
                match connect_async_tls_with_config(request, None, true, connector).await {
                    Ok(result) => result,
                    Err(error) => {
                        if let Some(failure) = pin_failure
                            .and_then(|failure| failure.lock().ok().and_then(|recorded| *recorded))
                        {
                            return Err(failure.into_error());
                        }
                        return Err(RealtimeError::Transport(error.to_string()));
                    }
                };
            Ok(Box::new(TokioTungsteniteSession { stream }) as Box<dyn WebSocketSession>)
        })
    }
}

struct TokioTungsteniteSession {
    stream: WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
}

impl WebSocketSession for TokioTungsteniteSession {
    fn send_text<'a>(&'a mut self, text: String) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async move {
            self.stream
                .send(Message::Text(text.into()))
                .await
                .map_err(|error| RealtimeError::Transport(error.to_string()))
        })
    }

    fn send_pong<'a>(&'a mut self, payload: Vec<u8>) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async move {
            self.stream
                .send(Message::Pong(payload.into()))
                .await
                .map_err(|error| RealtimeError::Transport(error.to_string()))
        })
    }

    fn receive<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<WebSocketMessage>, RealtimeError>> {
        Box::pin(async move {
            match self.stream.next().await {
                None => Ok(None),
                Some(Ok(Message::Text(text))) => Ok(Some(WebSocketMessage::Text(text.to_string()))),
                Some(Ok(Message::Ping(payload))) => {
                    Ok(Some(WebSocketMessage::Ping(payload.to_vec())))
                }
                Some(Ok(Message::Pong(payload))) => {
                    Ok(Some(WebSocketMessage::Pong(payload.to_vec())))
                }
                Some(Ok(Message::Close(frame))) => Ok(Some(WebSocketMessage::Close(
                    frame.map(|frame| frame.reason.to_string()),
                ))),
                Some(Ok(Message::Binary(_))) => Err(RealtimeError::Protocol(
                    "binary WebSocket frames are not accepted".to_owned(),
                )),
                Some(Ok(Message::Frame(_))) => Ok(None),
                Some(Err(error)) => Err(RealtimeError::Transport(error.to_string())),
            }
        })
    }

    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async move {
            self.stream
                .close(None)
                .await
                .map_err(|error| RealtimeError::Transport(error.to_string()))
        })
    }
}
