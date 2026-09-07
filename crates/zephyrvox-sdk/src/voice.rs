//! High-level voice-session lifecycle bound to HTTP and realtime authority.

use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{Mutex, broadcast, watch};

use zephyrvox_http::{ApiClient, RequestOptions, VoiceLeaveRequest};
use zephyrvox_realtime::ControlConnection;
use zephyrvox_types::{CodecCapability, InboundMedia, OutboundMedia, Snowflake, VoiceSessionId};
use zephyrvox_voice::{SessionKeyCache, VoiceSession as UdpVoiceSession, VoiceStatus};

use crate::{SdkError, voice_api::VoiceIntent};

/// A voice session created by an authenticated HTTP join and owned by the
/// current realtime control connection.
///
/// The facade binds the low-level UDP transport to its channel and codec
/// metadata.  It does not decode or mix media; hosts select a
/// [`crate::AudioCodec`] and use [`Self::send`] or [`Self::recv`] with encoded
/// frames.  Cloned handles share one lifecycle and one receive cursor.
#[derive(Clone)]
pub struct VoiceSession {
    pub(crate) inner: Arc<VoiceSessionInner>,
}

/// Shared state for the public voice handle.
pub(crate) struct VoiceSessionInner {
    pub(crate) channel_id: Snowflake,
    pub(crate) codec: CodecCapability,
    pub(crate) transport: UdpVoiceSession,
    pub(crate) api: ApiClient,
    pub(crate) control: ControlConnection,
    pub(crate) control_generation: u64,
    pub(crate) voice_intent: Arc<StdMutex<Option<VoiceIntent>>>,
    pub(crate) key_cache: SessionKeyCache,
    teardown_lock: Mutex<()>,
    left: AtomicBool,
}

/// Construction values for a fully negotiated facade voice session.
pub(crate) struct VoiceSessionParts {
    /// Voice channel selected by the host.
    pub(crate) channel_id: Snowflake,
    /// Metadata capability used by the host decoder.
    pub(crate) codec: CodecCapability,
    /// Started low-level UDP worker.
    pub(crate) transport: UdpVoiceSession,
    /// HTTP client used for the eventual leave mutation.
    pub(crate) api: ApiClient,
    /// Control connection that owns the server-side authority.
    pub(crate) control: ControlConnection,
    /// Generation of `control` at join time.
    pub(crate) control_generation: u64,
    /// Shared remembered rejoin intent.
    pub(crate) voice_intent: Arc<StdMutex<Option<VoiceIntent>>>,
    /// Zeroizing session-key cache.
    pub(crate) key_cache: SessionKeyCache,
}

impl VoiceSession {
    /// Creates a facade handle around an already-started UDP transport.
    pub(crate) fn new(parts: VoiceSessionParts) -> Self {
        Self {
            inner: Arc::new(VoiceSessionInner {
                channel_id: parts.channel_id,
                codec: parts.codec,
                transport: parts.transport,
                api: parts.api,
                control: parts.control,
                control_generation: parts.control_generation,
                voice_intent: parts.voice_intent,
                key_cache: parts.key_cache,
                teardown_lock: Mutex::new(()),
                left: AtomicBool::new(false),
            }),
        }
    }

    /// Returns the channel that owns this voice session.
    pub fn channel_id(&self) -> Snowflake {
        self.inner.channel_id
    }

    /// Returns the server-assigned voice session identifier.
    pub fn session_id(&self) -> VoiceSessionId {
        self.inner.transport.session_id()
    }

    /// Returns the codec capability selected from server metadata.
    pub fn codec(&self) -> &CodecCapability {
        &self.inner.codec
    }

    /// Returns the maximum encoded media payload accepted by this session.
    pub fn max_payload(&self) -> usize {
        self.inner.transport.max_payload()
    }

    /// Reports whether this session is encrypted at the UDP packet layer.
    pub fn encrypted(&self) -> bool {
        self.inner.transport.encrypted()
    }

    /// Returns the advertised Unix-millisecond session expiry.
    pub fn expires_at(&self) -> i64 {
        self.inner.transport.expires_at()
    }

    /// Returns the control-connection generation that owns this session.
    pub(crate) fn connection_generation(&self) -> u64 {
        self.inner.control_generation
    }

    /// Returns the current UDP lifecycle status.
    pub fn status(&self) -> VoiceStatus {
        self.inner.transport.status()
    }

    /// Subscribes to independent UDP lifecycle status changes.
    pub fn subscribe_status(&self) -> watch::Receiver<VoiceStatus> {
        self.inner.transport.subscribe_status()
    }

    /// Subscribes to received media with an independent bounded cursor.
    pub fn subscribe_media(&self) -> broadcast::Receiver<InboundMedia> {
        self.inner.transport.subscribe_media()
    }

    /// Sends one already encoded media frame.
    ///
    /// Codec selection, PCM conversion, and device capture remain outside the
    /// SDK.  The low-level voice crate validates the stream type and payload
    /// size before admitting the frame to its bounded queue.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Voice`] when the UDP session is inactive, revoked,
    /// full, or unable to encode/send the packet.
    pub async fn send(&self, frame: OutboundMedia) -> Result<(), SdkError> {
        self.inner
            .transport
            .send_frame(frame)
            .await
            .map_err(Into::into)
    }

    /// Receives the next media frame using this handle's shared cursor.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Voice`] when the session is closed or the bounded
    /// receive history has overwritten unread frames.
    pub async fn recv(&self) -> Result<InboundMedia, SdkError> {
        self.inner.transport.recv().await.map_err(Into::into)
    }

    /// Stops UDP locally without making an HTTP leave mutation.
    ///
    /// This is used during unified client shutdown and authority loss.  The
    /// server-side binding is then cleaned up by the control connection or by
    /// a later explicit [`Self::leave`] call.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Voice`] if the UDP worker cannot complete its
    /// terminal transition.
    pub async fn close(&self) -> Result<(), SdkError> {
        self.clear_voice_intent();
        self.stop_transport().await
    }

    /// Stops the UDP worker for an internal control-generation transition
    /// without clearing an explicitly opted-in reconnect intent.
    pub(crate) async fn close_transport(&self) -> Result<(), SdkError> {
        self.stop_transport().await
    }

    /// Stops the worker and clears the local encrypted key after serialization
    /// with other voice teardown operations.
    async fn stop_transport(&self) -> Result<(), SdkError> {
        let _guard = self.inner.teardown_lock.lock().await;
        let result = self.inner.transport.close().await;
        self.inner.key_cache.remove(self.session_id());
        result.map_err(Into::into)
    }

    /// Leaves the voice channel through HTTP after stopping the UDP worker.
    ///
    /// The control-connection ID is read immediately before the mutation.  A
    /// reconnect therefore cannot accidentally reuse an old generation's
    /// identity.  If the identity is unavailable, the local UDP session is
    /// still closed and the method returns [`SdkError::VoiceControlUnavailable`]
    /// without fabricating a new authority.
    ///
    /// # Errors
    ///
    /// Returns a voice, control-authority, or HTTP API error.  Repeated calls
    /// after a successful leave are idempotent.
    pub async fn leave(&self) -> Result<(), SdkError> {
        self.clear_voice_intent();
        let _guard = self.inner.teardown_lock.lock().await;
        if self.inner.left.load(Ordering::Acquire) {
            return Ok(());
        }

        let close_result = self.inner.transport.close().await;
        self.inner.key_cache.remove(self.session_id());
        let control_id = self.inner.control.id();
        let leave_result = match control_id {
            Some(control_id) => self
                .inner
                .api
                .channels()
                .leave_voice(
                    VoiceLeaveRequest {
                        voice_session_id: self.session_id(),
                    },
                    RequestOptions::mutation().with_control_connection(control_id),
                )
                .await
                .map(|_| ())
                .map_err(SdkError::from),
            None => Err(SdkError::VoiceControlUnavailable),
        };

        close_result.map_err(SdkError::from)?;
        leave_result?;
        self.inner.left.store(true, Ordering::Release);
        Ok(())
    }

    /// Waits until the UDP worker reaches a terminal status.
    pub async fn wait_until_stopped(&self) {
        self.inner.transport.wait_until_stopped().await;
    }

    /// Clears an automatic rejoin intent after an explicit host operation.
    fn clear_voice_intent(&self) {
        if let Ok(mut intent) = self.inner.voice_intent.lock() {
            if intent
                .as_ref()
                .is_some_and(|intent| intent.session_id == self.session_id())
            {
                *intent = None;
            }
        }
    }
}

impl std::fmt::Debug for VoiceSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VoiceSession")
            .field("channel_id", &self.channel_id())
            .field("session_id", &self.session_id())
            .field("codec", &self.codec())
            .field("status", &self.status())
            .finish()
    }
}
