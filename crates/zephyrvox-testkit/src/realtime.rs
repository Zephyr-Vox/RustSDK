//! In-memory WebSocket session and connector for synchronization tests.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::{Mutex, mpsc};

use zephyrvox_realtime::{
    AccessToken, BoxFuture, RealtimeError, WebSocketConnector, WebSocketMessage, WebSocketSession,
};

/// One fake WebSocket session consumed by [`FakeWebSocketConnector`].
pub struct FakeWebSocketSession {
    incoming: mpsc::Receiver<WebSocketMessage>,
    outgoing: mpsc::Sender<String>,
}

/// The test-side peer for a [`FakeWebSocketSession`].
pub struct FakeWebSocketPeer {
    incoming: mpsc::Sender<WebSocketMessage>,
    outgoing: mpsc::Receiver<String>,
}

/// Creates one bidirectional in-memory WebSocket pair.
pub fn websocket_pair(capacity: usize) -> (FakeWebSocketSession, FakeWebSocketPeer) {
    let (incoming_tx, incoming_rx) = mpsc::channel(capacity.max(1));
    let (outgoing_tx, outgoing_rx) = mpsc::channel(capacity.max(1));
    (
        FakeWebSocketSession {
            incoming: incoming_rx,
            outgoing: outgoing_tx,
        },
        FakeWebSocketPeer {
            incoming: incoming_tx,
            outgoing: outgoing_rx,
        },
    )
}

impl FakeWebSocketPeer {
    /// Sends one server-to-client transport message.
    pub async fn send(&self, message: WebSocketMessage) -> Result<(), RealtimeError> {
        self.incoming
            .send(message)
            .await
            .map_err(|_| RealtimeError::Closed)
    }

    /// Receives the next client-to-server text frame.
    pub async fn recv(&mut self) -> Option<String> {
        self.outgoing.recv().await
    }
}

impl WebSocketSession for FakeWebSocketSession {
    fn send_text<'a>(&'a mut self, text: String) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async move {
            self.outgoing
                .send(text)
                .await
                .map_err(|_| RealtimeError::Closed)
        })
    }

    fn send_pong<'a>(&'a mut self, _payload: Vec<u8>) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async { Ok(()) })
    }

    fn receive<'a>(&'a mut self) -> BoxFuture<'a, Result<Option<WebSocketMessage>, RealtimeError>> {
        Box::pin(async move { Ok(self.incoming.recv().await) })
    }

    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), RealtimeError>> {
        Box::pin(async { Ok(()) })
    }
}

/// A FIFO connector fake that creates one queued session per connection
/// attempt.
#[derive(Clone, Default)]
pub struct FakeWebSocketConnector {
    sessions: Arc<Mutex<VecDeque<FakeWebSocketSession>>>,
    attempts: Arc<AtomicUsize>,
}

impl FakeWebSocketConnector {
    /// Creates an empty connector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a session for the next connection attempt.
    pub async fn push_session(&self, session: FakeWebSocketSession) {
        self.sessions.lock().await.push_back(session);
    }

    /// Returns the number of connection attempts made by the realtime engine.
    pub fn attempts(&self) -> usize {
        self.attempts.load(Ordering::Acquire)
    }
}

impl WebSocketConnector for FakeWebSocketConnector {
    fn connect(
        &self,
        _url: url::Url,
        _access_token: AccessToken,
    ) -> BoxFuture<'static, Result<Box<dyn WebSocketSession>, RealtimeError>> {
        let sessions = Arc::clone(&self.sessions);
        let attempts = Arc::clone(&self.attempts);
        Box::pin(async move {
            attempts.fetch_add(1, Ordering::AcqRel);
            let session = sessions
                .lock()
                .await
                .pop_front()
                .ok_or(RealtimeError::Closed)?;
            Ok(Box::new(session) as Box<dyn WebSocketSession>)
        })
    }
}
