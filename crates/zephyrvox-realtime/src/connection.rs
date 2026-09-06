use std::{
    collections::HashMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::sync::{Mutex, broadcast, mpsc, oneshot, watch};
use url::Url;

use crate::{
    AccessToken, ClientEvent, CommandAck, CommandError, ConnectionStatus, PresenceCommand,
    RealtimeConfig, RealtimeError, SnapshotProvider, StateStore, WebSocketConnector, engine,
    frame::{encode_auth_update, encode_presence_set},
};
use zephyrvox_types::{ControlConnectionId, Presence};
use zephyrvox_wire::{ServerCard, TransportScheme};

/// A clonable handle to one authenticated WebSocket control connection.
///
/// The handle is cheap to clone. One Tokio task owns the socket and is the
/// only WebSocket reader/writer; clones communicate with that task through
/// bounded channels and request-correlated command waiters. Dropping a handle
/// does not close the connection; call [`Self::close`] when the host owns the
/// connection lifecycle.
#[derive(Clone)]
pub struct ControlConnection {
    pub(crate) inner: Arc<ConnectionInner>,
}

/// Shared connection state used by the public handle and its Tokio task.
pub(crate) struct ConnectionInner {
    pub(crate) provider: Arc<dyn SnapshotProvider>,
    pub(crate) config: RealtimeConfig,
    pub(crate) connector: Arc<dyn WebSocketConnector>,
    pub(crate) websocket_url: Url,
    pub(crate) state: StateStore,
    pub(crate) status_tx: watch::Sender<ConnectionStatus>,
    pub(crate) event_tx: broadcast::Sender<ClientEvent>,
    pub(crate) outbound: Mutex<Option<mpsc::Sender<String>>>,
    pub(crate) pending: Mutex<HashMap<String, oneshot::Sender<Result<CommandAck, RealtimeError>>>>,
    pub(crate) sequence: AtomicU64,
    pub(crate) stop_tx: watch::Sender<bool>,
    pub(crate) stopped: AtomicBool,
    pub(crate) control_id: RwLock<Option<ControlConnectionId>>,
    pub(crate) access_expires_at: AtomicI64,
}

impl ControlConnection {
    /// Connects using the production Tokio/tungstenite transport selected by
    /// `card`.
    ///
    /// The method obtains an initial provider snapshot before starting the
    /// socket, so [`Self::state`] is always a complete projection. It returns after
    /// the background Tokio task has been scheduled; use [`Self::wait_until_live`]
    /// when the caller needs the replay handoff to finish before continuing.
    ///
    /// # Errors
    ///
    /// Returns the provider or configuration error encountered while obtaining
    /// the initial snapshot or constructing the WebSocket URL.
    pub async fn connect(
        provider: Arc<dyn SnapshotProvider>,
        card: ServerCard,
        config: RealtimeConfig,
    ) -> Result<Self, RealtimeError> {
        let connector = Arc::new(crate::TokioTungsteniteConnector::new(card.clone()));
        Self::connect_with_connector(provider, card, config, connector).await
    }

    /// Connects with a caller-provided transport factory.
    ///
    /// This is the supported seam for deterministic fake WebSocket sessions,
    /// instrumented transports, and hosts that embed the SDK behind another
    /// socket abstraction. The connector remains shared by all reconnect
    /// attempts and must be safe to call concurrently.
    ///
    /// # Errors
    ///
    /// Returns the provider or configuration error encountered before the
    /// background connection task is started.
    pub async fn connect_with_connector(
        provider: Arc<dyn SnapshotProvider>,
        card: ServerCard,
        config: RealtimeConfig,
        connector: Arc<dyn WebSocketConnector>,
    ) -> Result<Self, RealtimeError> {
        let snapshot = provider.snapshot().await?;
        let websocket_url = websocket_url(&card)?;
        let state = StateStore::from_snapshot(snapshot);
        let (status_tx, _) = watch::channel(ConnectionStatus::Disconnected);
        let (event_tx, _) = broadcast::channel(256);
        let (stop_tx, _) = watch::channel(false);
        let inner = Arc::new(ConnectionInner {
            provider,
            config,
            connector,
            websocket_url,
            state,
            status_tx,
            event_tx,
            outbound: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            sequence: AtomicU64::new(0),
            stop_tx,
            stopped: AtomicBool::new(false),
            control_id: RwLock::new(None),
            access_expires_at: AtomicI64::new(0),
        });
        tokio::spawn(engine::run(Arc::clone(&inner)));
        Ok(Self { inner })
    }

    /// Returns the atomic state store shared by all clones of this handle.
    pub fn state(&self) -> StateStore {
        self.inner.state.clone()
    }

    /// Returns the current connection status without waiting for a change.
    pub fn status(&self) -> ConnectionStatus {
        *self.inner.status_tx.borrow()
    }

    /// Returns the current server-assigned control connection ID, if a socket
    /// has completed `connection.ready`.
    ///
    /// The value changes after a successful reconnect and becomes `None` when
    /// the connection is between socket generations. Callers must read it
    /// again before binding a new HTTP voice mutation.
    pub fn id(&self) -> Option<ControlConnectionId> {
        self.inner.control_id.read().ok().and_then(|id| *id)
    }

    /// Subscribes to lifecycle changes. The receiver always retains the most
    /// recent status and never accumulates an unbounded history.
    pub fn subscribe_status(&self) -> watch::Receiver<ConnectionStatus> {
        self.inner.status_tx.subscribe()
    }

    /// Subscribes to the combined connection, synchronization, state, and
    /// forward-compatible frame stream.
    pub fn subscribe_events(&self) -> broadcast::Receiver<ClientEvent> {
        self.inner.event_tx.subscribe()
    }

    /// Returns the server-assigned control connection ID, if a socket is ready.
    pub async fn control_connection_id(&self) -> Option<ControlConnectionId> {
        self.id()
    }

    /// Returns the Unix-millisecond access-lease expiry from the latest ready
    /// or `auth.update` acknowledgement, if one was advertised.
    pub fn access_expires_at(&self) -> Option<i64> {
        match self.inner.access_expires_at.load(Ordering::Acquire) {
            0 => None,
            expires_at => Some(expires_at),
        }
    }

    /// Waits until the current socket completes replay and becomes live.
    ///
    /// The wait observes automatic reconnects. It returns [`RealtimeError::Closed`]
    /// if the host closes the handle before a live phase is reached.
    pub async fn wait_until_live(&self) -> Result<(), RealtimeError> {
        let mut status = self.subscribe_status();
        loop {
            let current = *status.borrow();
            match current {
                ConnectionStatus::Live => return Ok(()),
                ConnectionStatus::Closed => return Err(RealtimeError::Closed),
                _ => status.changed().await.map_err(|_| RealtimeError::Closed)?,
            }
        }
    }

    /// Sends the global `presence.set` command and waits for its correlated
    /// acknowledgement. The state event produced by the server is delivered
    /// independently through [`StateStore::subscribe_events`].
    ///
    /// # Errors
    ///
    /// Returns [`RealtimeError::NotLive`] before replay completion,
    /// [`RealtimeError::CommandTimeout`] when the response deadline elapses,
    /// or [`RealtimeError::ServerCommand`] for a structured server rejection.
    pub async fn set_presence(&self, presence: Presence) -> Result<CommandAck, RealtimeError> {
        let command = PresenceCommand::from(presence);
        self.send_command(|request_id| encode_presence_set(request_id, &command))
            .await
    }

    /// Renews the current WebSocket access lease with an access token.
    ///
    /// The token is sent only in the `auth.update` frame. A refresh token is
    /// not accepted by this method and is never sent over WebSocket.
    pub async fn update_access(
        &self,
        access_token: AccessToken,
    ) -> Result<CommandAck, RealtimeError> {
        self.send_command(|request_id| encode_auth_update(request_id, &access_token))
            .await
    }

    /// Requests an orderly shutdown and waits for the background task to stop.
    ///
    /// All pending command futures complete with [`RealtimeError::Closed`].
    /// This method is idempotent and does not alter HTTP credentials.
    pub async fn close(&self) -> Result<(), RealtimeError> {
        if !self.inner.stopped.swap(true, Ordering::AcqRel) {
            let _ = self.inner.stop_tx.send(true);
        }
        let mut status = self.subscribe_status();
        while *status.borrow() != ConnectionStatus::Closed {
            if status.changed().await.is_err() {
                break;
            }
        }
        Ok(())
    }

    /// Inserts one bounded pending waiter, sends its payload through the
    /// single socket owner, and removes the waiter on ACK, close, or timeout.
    async fn send_command<F>(&self, encode: F) -> Result<CommandAck, RealtimeError>
    where
        F: FnOnce(&str) -> Result<String, RealtimeError>,
    {
        if self.status() != ConnectionStatus::Live {
            return Err(RealtimeError::NotLive);
        }
        let request_id = self.next_request_id();
        let payload = encode(&request_id)?;
        let (sender, receiver) = oneshot::channel();
        {
            let mut pending = self.inner.pending.lock().await;
            if pending.len() >= self.inner.config.maximum_pending_commands() {
                return Err(RealtimeError::Protocol(
                    "pending command capacity is full".to_owned(),
                ));
            }
            pending.insert(request_id.clone(), sender);
        }
        let outbound = self.inner.outbound.lock().await.clone();
        let Some(outbound) = outbound else {
            self.inner.pending.lock().await.remove(&request_id);
            return Err(RealtimeError::NotLive);
        };
        if outbound.send(payload).await.is_err() {
            self.inner.pending.lock().await.remove(&request_id);
            return Err(RealtimeError::Closed);
        }
        match tokio::time::timeout(self.inner.config.command_timeout(), receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(RealtimeError::Closed),
            Err(_) => {
                self.inner.pending.lock().await.remove(&request_id);
                Err(RealtimeError::CommandTimeout)
            }
        }
    }

    /// Creates a bounded, connection-local request identifier.
    fn next_request_id(&self) -> String {
        let sequence = self.inner.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis();
        format!("zv-{millis:x}-{sequence:x}")
    }
}

/// Builds the fixed `/api/v0/ws` endpoint without carrying card query values
/// or credentials in the URL.
fn websocket_url(card: &ServerCard) -> Result<Url, RealtimeError> {
    let scheme = match card.scheme() {
        TransportScheme::Plain => "ws",
        TransportScheme::Tls => "wss",
    };
    let host = if card.host().contains(':') {
        format!("[{}]", card.host())
    } else {
        card.host().to_owned()
    };
    Url::parse(&format!("{scheme}://{host}:{}/api/v0/ws", card.port()))
        .map_err(|error| RealtimeError::Protocol(error.to_string()))
}

/// Converts a server command failure into the public command error variant.
///
/// Kept as a small boundary helper so the engine never needs to construct a
/// request-specific error for a connection that has already been dropped.
pub(crate) fn command_failure(error: CommandError) -> RealtimeError {
    RealtimeError::ServerCommand(error)
}
