use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{
    Connector, WebSocketStream, connect_async_tls_with_config,
    tungstenite::{
        Error as TungsteniteError, Message, client::IntoClientRequest, protocol::WebSocketConfig,
    },
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
    /// Close notification supplied by a transport that does not expose its
    /// WebSocket close code.
    Close(Option<String>),
    /// Close frame including the protocol code when the transport exposes it.
    ///
    /// The connection coordinator uses server-defined terminal codes, such as
    /// authentication expiry, to stop reconnecting instead of treating every
    /// close as a transient network failure.
    CloseFrame {
        /// WebSocket close code, if the peer supplied a close frame.
        code: Option<u16>,
        /// Optional UTF-8 close reason.
        reason: Option<String>,
    },
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
            if !url_matches_card(&url, &card) {
                return Err(RealtimeError::Protocol(
                    "WebSocket URL does not match server card authority".to_owned(),
                ));
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
            let websocket_config = WebSocketConfig::default()
                .max_message_size(Some(256 * 1024))
                .max_frame_size(Some(256 * 1024));
            let (stream, _) = match connect_async_tls_with_config(
                request,
                Some(websocket_config),
                true,
                connector,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    if let Some(failure) = pin_failure
                        .and_then(|failure| failure.lock().ok().and_then(|recorded| *recorded))
                    {
                        return Err(failure.into_error());
                    }
                    return Err(map_upgrade_error(error));
                }
            };
            Ok(Box::new(TokioTungsteniteSession { stream }) as Box<dyn WebSocketSession>)
        })
    }
}

/// Confines the production connector to the card authority and fixed v1 path.
/// This check also protects callers that use the public connector directly
/// instead of going through [`crate::ControlConnection`].
fn url_matches_card(url: &Url, card: &ServerCard) -> bool {
    let expected_scheme = match card.scheme() {
        TransportScheme::Plain => "ws",
        TransportScheme::Tls => "wss",
    };
    url.scheme() == expected_scheme
        && url.host_str() == Some(card.host())
        && url.port_or_known_default() == Some(card.port())
        && url.path() == "/api/v0/ws"
        && url.query().is_none()
        && url.fragment().is_none()
        && url.username().is_empty()
        && url.password().is_none()
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
            loop {
                match self.stream.next().await {
                    None => return Ok(None),
                    Some(Ok(Message::Text(text))) => {
                        return Ok(Some(WebSocketMessage::Text(text.to_string())));
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        return Ok(Some(WebSocketMessage::Ping(payload.to_vec())));
                    }
                    Some(Ok(Message::Pong(payload))) => {
                        return Ok(Some(WebSocketMessage::Pong(payload.to_vec())));
                    }
                    Some(Ok(Message::Close(frame))) => {
                        return Ok(Some(WebSocketMessage::CloseFrame {
                            code: frame.as_ref().map(|frame| frame.code.into()),
                            reason: frame.map(|frame| frame.reason.to_string()),
                        }));
                    }
                    Some(Ok(Message::Binary(_))) => {
                        return Err(RealtimeError::Protocol(
                            "binary WebSocket frames are not accepted".to_owned(),
                        ));
                    }
                    Some(Ok(Message::Frame(_))) => {}
                    Some(Err(error)) => return Err(RealtimeError::Transport(error.to_string())),
                }
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

/// Maps an HTTP response from the WebSocket upgrade into the reconnect
/// classification used by the connection coordinator.
fn map_upgrade_error(error: TungsteniteError) -> RealtimeError {
    match error {
        TungsteniteError::Http(response) => match response.status().as_u16() {
            401 => RealtimeError::AuthenticationExpired,
            403 => RealtimeError::Protocol("WebSocket upgrade was forbidden".to_owned()),
            _ => RealtimeError::Transport(format!(
                "WebSocket upgrade failed with HTTP status {}",
                response.status()
            )),
        },
        error => RealtimeError::Transport(error.to_string()),
    }
}
