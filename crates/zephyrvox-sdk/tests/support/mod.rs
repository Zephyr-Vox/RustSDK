#![allow(dead_code)]

use std::{collections::VecDeque, sync::Arc};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use tokio::sync::{Mutex, mpsc};

use zephyrvox_sdk::{
    HttpError, HttpRequest, HttpResponse, HttpTransport, ServerCard, TransportScheme,
    WebSocketConnector, WebSocketMessage, WebSocketSession,
};

/// FIFO HTTP transport used by facade integration tests.
#[derive(Clone)]
pub struct QueueHttp {
    responses: Arc<Mutex<VecDeque<Result<HttpResponse, HttpError>>>>,
}

impl QueueHttp {
    /// Creates a queue containing the supplied successful responses.
    pub fn new(responses: impl IntoIterator<Item = HttpResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().map(Ok).collect())),
        }
    }
}

impl HttpTransport for QueueHttp {
    fn send(
        &self,
        _request: HttpRequest,
    ) -> zephyrvox_sdk::http::BoxFuture<'_, Result<HttpResponse, HttpError>> {
        let responses = Arc::clone(&self.responses);
        Box::pin(async move {
            responses.lock().await.pop_front().unwrap_or_else(|| {
                Err(HttpError::Transport(
                    "test HTTP response queue is empty".to_owned(),
                ))
            })
        })
    }
}

/// One fake WebSocket session owned by a [`FakeConnector`].
pub struct FakeSession {
    incoming: mpsc::Receiver<WebSocketMessage>,
    outgoing: mpsc::Sender<String>,
}

/// Creates a fake session and its server-side channels.
pub fn websocket_pair() -> (
    FakeSession,
    mpsc::Sender<WebSocketMessage>,
    mpsc::Receiver<String>,
) {
    let (server_tx, client_rx) = mpsc::channel(32);
    let (client_tx, server_rx) = mpsc::channel(32);
    (
        FakeSession {
            incoming: client_rx,
            outgoing: client_tx,
        },
        server_tx,
        server_rx,
    )
}

impl WebSocketSession for FakeSession {
    fn send_text<'a>(
        &'a mut self,
        text: String,
    ) -> zephyrvox_sdk::BoxFuture<'a, Result<(), zephyrvox_sdk::RealtimeError>> {
        Box::pin(async move {
            self.outgoing
                .send(text)
                .await
                .map_err(|_| zephyrvox_sdk::RealtimeError::Closed)
        })
    }

    fn send_pong<'a>(
        &'a mut self,
        _payload: Vec<u8>,
    ) -> zephyrvox_sdk::BoxFuture<'a, Result<(), zephyrvox_sdk::RealtimeError>> {
        Box::pin(async { Ok(()) })
    }

    fn receive<'a>(
        &'a mut self,
    ) -> zephyrvox_sdk::BoxFuture<'a, Result<Option<WebSocketMessage>, zephyrvox_sdk::RealtimeError>>
    {
        Box::pin(async move { Ok(self.incoming.recv().await) })
    }

    fn close<'a>(
        &'a mut self,
    ) -> zephyrvox_sdk::BoxFuture<'a, Result<(), zephyrvox_sdk::RealtimeError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A queued WebSocket connector used to drive one or more connection
/// generations deterministically.
#[derive(Clone, Default)]
pub struct FakeConnector {
    sessions: Arc<Mutex<VecDeque<FakeSession>>>,
}

impl FakeConnector {
    /// Creates an empty connector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues one session for the next connection attempt.
    pub async fn push(&self, session: FakeSession) {
        self.sessions.lock().await.push_back(session);
    }
}

impl WebSocketConnector for FakeConnector {
    fn connect(
        &self,
        _url: url::Url,
        _access_token: zephyrvox_sdk::AccessToken,
    ) -> zephyrvox_sdk::BoxFuture<
        'static,
        Result<Box<dyn WebSocketSession>, zephyrvox_sdk::RealtimeError>,
    > {
        let sessions = Arc::clone(&self.sessions);
        Box::pin(async move {
            let session = sessions
                .lock()
                .await
                .pop_front()
                .ok_or(zephyrvox_sdk::RealtimeError::Closed)?;
            Ok(Box::new(session) as Box<dyn WebSocketSession>)
        })
    }
}

/// Creates a valid plain server card for facade tests.
pub fn card() -> ServerCard {
    ServerCard::new("127.0.0.1", 18_080, TransportScheme::Plain, None, None).unwrap()
}

/// Encodes one JSON HTTP response.
pub fn response(value: serde_json::Value) -> HttpResponse {
    HttpResponse::new(
        200,
        [("content-type".to_owned(), "application/json".to_owned())],
        serde_json::to_vec(&value).unwrap(),
    )
}

/// Encodes one successful CommunityServer envelope.
pub fn envelope(data: serde_json::Value) -> HttpResponse {
    response(serde_json::json!({"code": 0, "message": "", "data": data}))
}

/// Creates metadata with one encrypted-capable audio stream.
pub fn metadata(port: u16) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": 1,
        "voice_endpoint": {"host": "127.0.0.1", "port": port},
        "codecs": [{"name": "opus", "clock_rate": 48000, "channels": 2, "ptime": [20]}],
        "voice_stream_types": [{"id": 1, "name": "audio", "mute_kind": "self"}],
        "features": []
    })
}

/// Creates a minimal successful login payload.
pub fn login() -> serde_json::Value {
    serde_json::json!({
        "access_token": "access-token",
        "refresh_token": "refresh-token",
        "expires_in": 3600,
        "user": {"id": "1", "username": "owner", "nickname": "Owner", "avatar": null}
    })
}

/// Creates a minimal authenticated state snapshot.
pub fn snapshot() -> serde_json::Value {
    serde_json::json!({
        "cursor": "cursor-0",
        "stream_epoch": "00000000000000000000000000000001",
        "geid": "0",
        "state_version": "1",
        "state": {
            "server": {"name": "Test", "version": "dev"},
            "self": {
                "user": {"id": "1", "username": "owner", "nickname": "Owner", "avatar": null},
                "server_role_keys": ["owner"],
                "server_permissions": ["*"],
                "presence": {"status": "online", "activity": null},
                "voice_authority": null
            },
            "users": [],
            "roles": [{"key": "owner", "display_name": "Owner", "rank": 0, "builtin": true}],
            "groups": [],
            "channels": [],
            "voice_memberships": []
        }
    })
}

/// Creates a voice join payload with either encrypted or plaintext media.
pub fn voice_join(channel_id: &str, encrypted: bool) -> serde_json::Value {
    serde_json::json!({
        "channel": {
            "id": channel_id,
            "group_id": null,
            "name": "Lobby",
            "mode": "voice",
            "temporary": false,
            "visibility": "public",
            "capacity": 16,
            "position": 1,
            "pinned": false,
            "version": "1"
        },
        "members": [],
        "voice": {
            "created": true,
            "session_id": "00000000000000000000000000000022",
            "key": if encrypted { serde_json::Value::String(STANDARD.encode([7_u8; 32])) } else { serde_json::Value::Null },
            "encrypted": encrypted,
            "warning": if encrypted { serde_json::Value::Null } else { serde_json::Value::String("plaintext_mode".to_owned()) },
            "max_payload": 100,
            "protocol_version": 1,
            "expires_at": 4102444800000_i64
        },
        "state_cursor": "cursor-1",
        "state_checkpoint": {
            "stream_epoch": "00000000000000000000000000000001",
            "geid": "1"
        }
    })
}
