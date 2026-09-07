use std::sync::{Arc, Mutex as StdMutex};

use bytes::Bytes;
use tokio::{
    net::UdpSocket,
    sync::{Mutex, broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
};

use crate::{
    PacketCodec, RevocationReason, SessionKeyCache, VoiceError, VoiceSessionConfig, VoiceStatus,
    worker::{SessionCommand, SessionInner, Termination},
};
use zephyrvox_types::{InboundMedia, OutboundMedia, StreamTypeId, VoiceEndpoint, VoiceSessionId};

const MEDIA_QUEUE_CAPACITY: usize = 128;

/// Aborts a worker if startup is canceled before the public handle is ready.
struct StartupTaskGuard(Option<JoinHandle<()>>);

impl StartupTaskGuard {
    /// Detaches the task after it has published a successful active status.
    fn disarm(&mut self) {
        self.0.take();
    }
}

impl Drop for StartupTaskGuard {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// A clonable handle to one already-negotiated UDP voice session.
///
/// The session owns one Tokio task and one connected UDP socket. Media sends
/// are admitted through a bounded command queue, while received frames are
/// published through a bounded broadcast channel. Dropping a handle does not
/// implicitly rejoin or revoke the server session; hosts should call
/// [`Self::close`] during their lifecycle teardown.
#[derive(Clone)]
pub struct VoiceSession {
    inner: Arc<SessionInner>,
}

impl VoiceSession {
    /// Resolves the configured endpoint, binds an ephemeral local UDP port,
    /// sends the first heartbeat, and starts receiving media.
    ///
    /// This method requires a validated HTTP join result. UDP never creates a
    /// business session by itself.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError`] when endpoint resolution, key resolution,
    /// initial heartbeat, or task startup fails.
    pub async fn connect(config: VoiceSessionConfig) -> Result<Self, VoiceError> {
        Self::connect_with_optional_cache(config, None).await
    }

    /// Connects a voice session while allowing encrypted session-key reuse.
    ///
    /// A newly created encrypted session inserts its one-time key into
    /// `key_cache`. A reused session must find its key there; a missing key is
    /// reported explicitly and never replaced with plaintext.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::KeyUnavailable`] when a reused encrypted session
    /// is not present in the cache, or another [`VoiceError`] for startup
    /// failures.
    pub async fn connect_with_key_cache(
        config: VoiceSessionConfig,
        key_cache: SessionKeyCache,
    ) -> Result<Self, VoiceError> {
        Self::connect_with_optional_cache(config, Some(key_cache)).await
    }

    /// Starts a voice session on an already connected UDP socket.
    ///
    /// This constructor is useful for embedded hosts that own socket binding
    /// and for deterministic integration tests. The socket must already be
    /// connected to the server endpoint, so the operating system filters
    /// datagrams from unrelated sources.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::Endpoint`] if the socket is not connected, or a
    /// startup error from configuration, encryption, or the first heartbeat.
    pub async fn from_connected_socket(
        config: VoiceSessionConfig,
        socket: UdpSocket,
    ) -> Result<Self, VoiceError> {
        if socket.peer_addr().is_err() {
            return Err(VoiceError::Endpoint(
                "voice UDP socket must be connected before use".to_owned(),
            ));
        }
        Self::start(config, socket, None).await
    }

    /// Starts a voice session on a connected UDP socket with key-cache reuse.
    ///
    /// # Errors
    ///
    /// Returns the same startup errors as [`Self::from_connected_socket`].
    pub async fn from_connected_socket_with_key_cache(
        config: VoiceSessionConfig,
        socket: UdpSocket,
        key_cache: SessionKeyCache,
    ) -> Result<Self, VoiceError> {
        if socket.peer_addr().is_err() {
            return Err(VoiceError::Endpoint(
                "voice UDP socket must be connected before use".to_owned(),
            ));
        }
        Self::start(config, socket, Some(key_cache)).await
    }

    /// Returns the negotiated voice session identifier.
    pub fn session_id(&self) -> VoiceSessionId {
        self.inner.session_id
    }

    /// Returns the advertised UDP endpoint used by this session.
    pub fn endpoint(&self) -> &VoiceEndpoint {
        &self.inner.endpoint
    }

    /// Reports whether this session uses AES-GCM packet protection.
    pub fn encrypted(&self) -> bool {
        self.inner.encrypted
    }

    /// Returns the negotiated encoded media payload limit.
    pub fn max_payload(&self) -> usize {
        self.inner.max_payload
    }

    /// Returns the server-advertised voice-session expiry in Unix milliseconds.
    ///
    /// The transport exposes this value for host lifecycle policy but does not
    /// locally force a disconnect at the deadline: server heartbeat activity,
    /// control-plane leave, and revocation remain authoritative.
    pub fn expires_at(&self) -> i64 {
        self.inner.expires_at
    }

    /// Returns the current lifecycle status without waiting for a change.
    pub fn status(&self) -> VoiceStatus {
        *self.inner.status_tx.borrow()
    }

    /// Subscribes to latest-only lifecycle status changes.
    pub fn subscribe_status(&self) -> watch::Receiver<VoiceStatus> {
        self.inner.status_tx.subscribe()
    }

    /// Subscribes to received media frames.
    ///
    /// A slow subscriber receives `RecvError::Lagged`; the UDP receive loop is
    /// never blocked by application media processing.
    pub fn subscribe_media(&self) -> broadcast::Receiver<InboundMedia> {
        let sender = self
            .inner
            .media_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(sender) = sender.as_ref() {
            return sender.subscribe();
        }
        closed_media_receiver()
    }

    /// Receives the next media frame from this session's shared media cursor.
    ///
    /// Cloned [`VoiceSession`] handles share this cursor, so concurrent callers
    /// divide frames between themselves. Hosts that need independent consumers
    /// should call [`Self::subscribe_media`] and manage each receiver's cursor
    /// separately. A lagging cursor reports [`VoiceError::MediaLagged`] rather
    /// than blocking the UDP worker or silently skipping the loss.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::MediaLagged`] when the bounded media history has
    /// overwritten unread frames, or [`VoiceError::Closed`] after the worker
    /// and its media channel have stopped.
    pub async fn recv(&self) -> Result<InboundMedia, VoiceError> {
        let mut receiver = self.inner.media_rx.lock().await;
        receiver.recv().await.map_err(|error| match error {
            broadcast::error::RecvError::Lagged(missed) => VoiceError::MediaLagged { missed },
            broadcast::error::RecvError::Closed => VoiceError::Closed,
        })
    }

    /// Sends an already encoded media payload on one registered stream type.
    ///
    /// The session allocates the transport sequence and maintains an
    /// independent wrapping `u16` channel sequence per stream. The payload is
    /// copied into the bounded command queue before this method awaits the
    /// UDP write.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::NotActive`] when the session is not active,
    /// [`VoiceError::Revoked`] when the server withdrew voice authority,
    /// [`VoiceError::QueueFull`] when the bounded media lane is full, or a
    /// packet/transport error when local validation or the UDP write fails.
    pub async fn send(
        &self,
        stream_type: StreamTypeId,
        payload: impl AsRef<[u8]>,
    ) -> Result<(), VoiceError> {
        match self.status() {
            VoiceStatus::Active => {}
            VoiceStatus::Revoked(reason) => return Err(VoiceError::Revoked(reason)),
            VoiceStatus::Disconnected => return Err(VoiceError::Closed),
            VoiceStatus::Idle | VoiceStatus::Joining | VoiceStatus::Disconnecting => {
                return Err(VoiceError::NotActive);
            }
        }
        let payload = payload.as_ref();
        PacketCodec::validate_media_values(
            &self.inner.allowed_stream_types,
            self.inner.max_payload,
            stream_type,
            payload.len(),
        )?;
        let (response, result) = oneshot::channel();
        self.inner
            .command_tx
            .try_send(SessionCommand::Send {
                stream_type,
                payload: Bytes::copy_from_slice(payload),
                response,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => VoiceError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => VoiceError::Closed,
            })?;
        result.await.map_err(|_| VoiceError::Closed)?
    }

    /// Sends the payload from a typed outbound media value.
    ///
    /// # Errors
    ///
    /// Returns the same lifecycle, queue, packet, and transport errors as
    /// [`Self::send`].
    pub async fn send_frame(&self, frame: OutboundMedia) -> Result<(), VoiceError> {
        self.send(frame.stream_type, frame.payload).await
    }

    /// Stops the UDP task and marks the session disconnected.
    ///
    /// This is an orderly local stop. It does not perform HTTP leave and does
    /// not attempt to rejoin; the host owns that control-plane decision.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::Closed`] if the worker has already stopped before
    /// it can observe the termination request.
    pub async fn close(&self) -> Result<(), VoiceError> {
        self.request_termination(Termination::Close).await
    }

    /// Stops the UDP task and marks the session revoked without rejoining.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::Closed`] if the worker has already stopped before
    /// it can observe the termination request.
    pub async fn revoke(&self, reason: RevocationReason) -> Result<(), VoiceError> {
        self.request_termination(Termination::Revoke(reason)).await
    }

    /// Stops the UDP task for a non-revocation transport/control disconnect.
    ///
    /// # Errors
    ///
    /// Returns [`VoiceError::Closed`] if the worker has already stopped before
    /// it can observe the termination request.
    pub async fn disconnect(&self) -> Result<(), VoiceError> {
        self.request_termination(Termination::Disconnect).await
    }

    /// Waits until this session reaches `Disconnected` or `Revoked`.
    pub async fn wait_until_stopped(&self) {
        let mut status = self.subscribe_status();
        while !status.borrow().is_terminal() {
            if status.changed().await.is_err() {
                break;
            }
        }
    }

    /// Connects through the endpoint resolver while preserving optional key
    /// cache ownership for the worker lifecycle.
    async fn connect_with_optional_cache(
        config: VoiceSessionConfig,
        key_cache: Option<SessionKeyCache>,
    ) -> Result<Self, VoiceError> {
        let endpoint = config.endpoint().clone();
        let socket = crate::worker::connect_socket(&endpoint).await?;
        Self::start(config, socket, key_cache).await
    }

    /// Creates the worker task after validating the negotiated packet codec.
    async fn start(
        config: VoiceSessionConfig,
        socket: UdpSocket,
        key_cache: Option<SessionKeyCache>,
    ) -> Result<Self, VoiceError> {
        let endpoint = config.endpoint().clone();
        let codec = PacketCodec::from_config(&config, key_cache.as_ref())?;
        let allowed_stream_types = codec.allowed_stream_types;
        let (status_tx, _) = watch::channel(VoiceStatus::Joining);
        let (media_tx, _) = broadcast::channel(MEDIA_QUEUE_CAPACITY);
        let media_rx = media_tx.subscribe();
        let (command_tx, command_rx) = mpsc::channel(MEDIA_QUEUE_CAPACITY);
        let (stop_tx, stop_rx) = watch::channel(None);
        let (started_tx, started_rx) = oneshot::channel();
        let inner = Arc::new(SessionInner {
            session_id: config.session_id(),
            endpoint,
            expires_at: config.expires_at(),
            encrypted: config.encrypted(),
            max_payload: config.max_payload(),
            status_tx,
            media_tx: StdMutex::new(Some(media_tx)),
            media_rx: Mutex::new(media_rx),
            command_tx,
            stop_tx,
            key_cache,
            allowed_stream_types,
        });
        let task_inner = Arc::clone(&inner);
        let task = tokio::spawn(crate::worker::run(
            task_inner, socket, codec, command_rx, stop_rx, started_tx,
        ));
        let mut startup_guard = StartupTaskGuard(Some(task));
        match started_rx.await {
            Ok(Ok(())) => {
                startup_guard.disarm();
                Ok(Self { inner })
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(VoiceError::Closed),
        }
    }

    /// Requests one terminal lifecycle transition and waits for the worker to
    /// publish it.
    async fn request_termination(&self, termination: Termination) -> Result<(), VoiceError> {
        if self.status().is_terminal() {
            return Ok(());
        }
        if matches!(termination, Termination::Close | Termination::Disconnect) {
            crate::worker::publish_status(&self.inner, VoiceStatus::Disconnecting);
        }
        self.inner
            .stop_tx
            .send(Some(termination))
            .map_err(|_| VoiceError::Closed)?;
        self.wait_until_stopped().await;
        Ok(())
    }
}

/// Creates a receiver whose sender has already been dropped for a stopped
/// session, preserving the post-close behavior of [`VoiceSession::recv`].
fn closed_media_receiver() -> broadcast::Receiver<InboundMedia> {
    let (sender, receiver) = broadcast::channel(1);
    drop(sender);
    receiver
}
