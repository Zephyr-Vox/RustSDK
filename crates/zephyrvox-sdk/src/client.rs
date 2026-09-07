//! High-level client lifecycle and domain orchestration.

use std::sync::{
    Arc, Mutex as StdMutex, RwLock as StdRwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use tokio::{
    sync::{Mutex, broadcast, watch},
    task::JoinHandle,
};

use zephyrvox_http::{ApiClient, LoginRequest, LoginResult, ReqwestTransport};
use zephyrvox_realtime::{
    ClientEvent as RealtimeClientEvent, CommandAck, ConnectionStatus, ControlConnection,
    RealtimeConfig, WebSocketConnector,
};
use zephyrvox_types::{ClientState, Metadata, Presence};
use zephyrvox_voice::SessionKeyCache;
use zephyrvox_wire::ServerCard;

use crate::{
    ClientConfig, ClientEvent, EventStream, SdkError, VoiceSession, provider::provider,
    voice_api::VoiceIntent,
};

/// A clonable, Tokio-first facade for HTTP, WebSocket, and UDP voice.
///
/// `Client` owns the coordination tasks that relay realtime events and bind
/// voice to control authority.  Hosts do not need to start transport reader,
/// writer, refresh, reconnect, or UDP worker tasks themselves.  Clones share
/// the same lifecycle; call [`Self::shutdown`] once the host is done with the
/// client.
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

/// State shared by all clones of one facade client.
pub(crate) struct ClientInner {
    pub(crate) api: ApiClient,
    pub(crate) card: ServerCard,
    pub(crate) event_tx: broadcast::Sender<ClientEvent>,
    pub(crate) metadata: StdRwLock<Option<Metadata>>,
    pub(crate) state: StdRwLock<Option<zephyrvox_realtime::StateStore>>,
    pub(crate) control: Mutex<Option<ControlConnection>>,
    pub(crate) voice: Mutex<Option<VoiceSession>>,
    pub(crate) voice_intent: Arc<StdMutex<Option<VoiceIntent>>>,
    pub(crate) voice_gate: Mutex<()>,
    pub(crate) tasks: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) shutdown_gate: Mutex<()>,
    pub(crate) stopped: AtomicBool,
    pub(crate) connection_generation: AtomicU64,
    pub(crate) key_cache: SessionKeyCache,
    pub(crate) realtime_config: RealtimeConfig,
    pub(crate) auto_rejoin_voice: bool,
}

impl Client {
    /// Creates a production client using the transport policy in `config`.
    ///
    /// TLS verification and SPKI pinning remain fixed by the server card;
    /// there is no insecure certificate bypass or plaintext fallback.  The
    /// configured credential store is shared by every HTTP and realtime
    /// operation.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Configuration`] for invalid timeout values and an
    /// HTTP transport error when the underlying client cannot be built.
    pub fn new(config: ClientConfig) -> Result<Self, SdkError> {
        config.validate()?;
        let card = config.server_card.clone();
        let transport = Arc::new(ReqwestTransport::new(&card, config.request_timeout)?);
        let credential_store = Arc::clone(&config.credential_store);
        let api = ApiClient::with_transport(card.clone(), transport, credential_store)?;
        Self::from_api(config, api)
    }

    /// Creates a facade around an existing typed HTTP client.
    ///
    /// This is the injection seam for test transports and embedded hosts that
    /// already own an [`ApiClient`].  The two server cards must be equal so a
    /// custom WebSocket or UDP operation cannot silently use another
    /// authority.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Configuration`] when the timeout or server-card
    /// binding is invalid.
    pub fn with_api_client(config: ClientConfig, api: ApiClient) -> Result<Self, SdkError> {
        config.validate()?;
        if api.server_card() != &config.server_card {
            return Err(SdkError::configuration(
                "facade and HTTP client use different server cards",
            ));
        }
        Self::from_api(config, api)
    }

    /// Returns the typed HTTP facade shared by this client.
    ///
    /// The returned handle is cheap to clone and shares credentials and
    /// refresh singleflight with the client.  Lifecycle-sensitive operations
    /// should use the methods on [`Client`], which reject calls after
    /// [`Self::shutdown`].
    pub fn api(&self) -> ApiClient {
        self.inner.api.clone()
    }

    /// Discovers and caches the server's protocol and media capabilities.
    ///
    /// Metadata is unauthenticated and is validated by the HTTP adapter before
    /// it is cached.  The same protocol version covers HTTP, WebSocket, and
    /// UDP; a mismatch is returned as an error and never downgraded.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Closed`] after shutdown or the typed HTTP/wire
    /// error returned by metadata discovery.
    pub async fn discover(&self) -> Result<Metadata, SdkError> {
        self.ensure_open()?;
        if let Some(metadata) = self
            .inner
            .metadata
            .read()
            .ok()
            .and_then(|metadata| metadata.clone())
        {
            return Ok(metadata);
        }
        let metadata = self.inner.api.metadata().await?;
        if let Ok(mut cached) = self.inner.metadata.write() {
            *cached = Some(metadata.clone());
        }
        Ok(metadata)
    }

    /// Logs in and stores the rotated access/refresh pair atomically.
    ///
    /// Refresh tokens remain inside the HTTP credential store and are never
    /// handed to the WebSocket or UDP layers.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Closed`] after shutdown or the typed HTTP/auth
    /// error returned by the login endpoint.
    pub async fn login(&self, request: LoginRequest) -> Result<LoginResult, SdkError> {
        self.ensure_open()?;
        Ok(self.inner.api.auth().login(request).await?)
    }

    /// Opens or returns the single facade-owned realtime control connection.
    ///
    /// The connection obtains its initial HTTP snapshot before the WebSocket
    /// task starts.  A reconnect changes the server-assigned control ID; hosts
    /// must read the ID again before a voice mutation.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Closed`] after shutdown, an authentication error
    /// when no access token is available, or a typed realtime/HTTP error.
    pub async fn connect(&self) -> Result<ControlConnection, SdkError> {
        self.connect_with_optional_connector(None).await
    }

    /// Opens the control connection with a caller-provided WebSocket factory.
    ///
    /// Production callers normally use [`Self::connect`].  This method keeps
    /// the fake/instrumented transport seam at the facade boundary so a host
    /// does not need to coordinate the realtime crate directly.
    ///
    /// # Errors
    ///
    /// Returns the same lifecycle, authentication, and transport errors as
    /// [`Self::connect`].
    pub async fn connect_with_connector(
        &self,
        connector: Arc<dyn WebSocketConnector>,
    ) -> Result<ControlConnection, SdkError> {
        self.connect_with_optional_connector(Some(connector)).await
    }

    /// Returns the latest complete state receiver after a control connection
    /// has been created.
    ///
    /// `None` is returned before [`Self::connect`] has installed an initial
    /// snapshot.  Each receiver observes atomic `Arc<ClientState>` updates;
    /// no partially applied state is exposed.
    pub fn state(&self) -> Option<watch::Receiver<Arc<ClientState>>> {
        self.inner.state.read().ok().and_then(|state| {
            state
                .as_ref()
                .map(zephyrvox_realtime::StateStore::subscribe_state)
        })
    }

    /// Returns the facade's current voice session, if one is active locally.
    ///
    /// The returned handle is a cheap clone. It is also the handle a host
    /// should reacquire after an opted-in automatic voice rejoin, because the
    /// rejoin necessarily creates a new local UDP session object.
    pub async fn voice_session(&self) -> Option<VoiceSession> {
        self.inner.voice.lock().await.clone()
    }

    /// Creates an event cursor at the current client event boundary.
    pub fn events(&self) -> EventStream {
        EventStream::from(self.inner.event_tx.subscribe())
    }

    /// Returns the currently stored control connection, if one exists.
    pub async fn control_connection(&self) -> Option<ControlConnection> {
        self.inner.control.lock().await.clone()
    }

    /// Sends the global presence command through the active control socket.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::NotConnected`], [`SdkError::ControlNotLive`], or a
    /// typed realtime command error.
    pub async fn set_presence(&self, presence: Presence) -> Result<CommandAck, SdkError> {
        let control = self.current_control().await?;
        if control.status() != ConnectionStatus::Live {
            return Err(SdkError::ControlNotLive);
        }
        Ok(control.set_presence(presence).await?)
    }

    /// Builds the shared client state from a typed HTTP facade.
    fn from_api(config: ClientConfig, api: ApiClient) -> Result<Self, SdkError> {
        let realtime_config = RealtimeConfig::new().with_reconnect_policy(config.reconnect);
        let (event_tx, _) = broadcast::channel(EventStream::capacity());
        Ok(Self {
            inner: Arc::new(ClientInner {
                api,
                card: config.server_card,
                event_tx,
                metadata: StdRwLock::new(None),
                state: StdRwLock::new(None),
                control: Mutex::new(None),
                voice: Mutex::new(None),
                voice_intent: Arc::new(StdMutex::new(None)),
                voice_gate: Mutex::new(()),
                tasks: Mutex::new(Vec::new()),
                shutdown_gate: Mutex::new(()),
                stopped: AtomicBool::new(false),
                connection_generation: AtomicU64::new(0),
                key_cache: SessionKeyCache::new(),
                realtime_config,
                auto_rejoin_voice: config.auto_rejoin_voice,
            }),
        })
    }

    /// Connects while serializing replacement of an already closed handle.
    async fn connect_with_optional_connector(
        &self,
        connector: Option<Arc<dyn WebSocketConnector>>,
    ) -> Result<ControlConnection, SdkError> {
        self.ensure_open()?;
        let mut current = self.inner.control.lock().await;
        if let Some(control) = current.as_ref()
            && control.status() != ConnectionStatus::Closed
        {
            return Ok(control.clone());
        }
        *current = None;

        let provider = provider(self.inner.api.clone());
        let connection = match connector {
            Some(connector) => {
                ControlConnection::connect_with_connector(
                    provider,
                    self.inner.card.clone(),
                    self.inner.realtime_config,
                    connector,
                )
                .await?
            }
            None => {
                ControlConnection::connect(
                    provider,
                    self.inner.card.clone(),
                    self.inner.realtime_config,
                )
                .await?
            }
        };
        if self.inner.stopped.load(Ordering::Acquire) {
            let _ = connection.close().await;
            return Err(SdkError::Closed);
        }
        let generation = self
            .inner
            .connection_generation
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if let Ok(mut state) = self.inner.state.write() {
            *state = Some(connection.state());
        }
        self.spawn_event_relay(connection.subscribe_events(), generation)
            .await;
        *current = Some(connection.clone());
        Ok(connection)
    }

    /// Returns the current non-closed control handle.
    pub(crate) async fn current_control(&self) -> Result<ControlConnection, SdkError> {
        self.ensure_open()?;
        let control = self.inner.control.lock().await.clone();
        match control {
            Some(control) if control.status() != ConnectionStatus::Closed => Ok(control),
            _ => Err(SdkError::NotConnected),
        }
    }

    /// Starts one facade relay for a realtime event receiver.
    async fn spawn_event_relay(
        &self,
        receiver: broadcast::Receiver<RealtimeClientEvent>,
        generation: u64,
    ) {
        let weak = Arc::downgrade(&self.inner);
        let sender = self.inner.event_tx.clone();
        let task = tokio::spawn(async move {
            crate::events::relay_realtime_events(weak, receiver, sender, generation).await;
        });
        self.inner.tasks.lock().await.push(task);
    }

    /// Stops the current UDP worker only when it belongs to an expected
    /// control-connection generation.
    pub(crate) async fn stop_voice_transport_for(
        &self,
        expected_generation: Option<u64>,
    ) -> Result<(), SdkError> {
        let _voice_gate = self.inner.voice_gate.lock().await;
        let session = {
            let mut voice = self.inner.voice.lock().await;
            let matches_generation =
                voice
                    .as_ref()
                    .is_some_and(|session| match expected_generation {
                        Some(generation) => session.connection_generation() == generation,
                        None => true,
                    });
            if matches_generation {
                voice.take()
            } else {
                None
            }
        };
        match session {
            Some(session) => session.close_transport().await,
            None => Ok(()),
        }
    }
}
