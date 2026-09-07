//! Voice join and leave orchestration for the high-level client.

use std::sync::{Arc, atomic::Ordering};

use base64::{Engine as _, engine::general_purpose::STANDARD};

use zephyrvox_http::{RequestOptions, VoiceJoinRequest};
use zephyrvox_realtime::ConnectionStatus;
use zephyrvox_types::Snowflake;
use zephyrvox_voice::{SessionKey, VoiceSession as UdpVoiceSession, VoiceSessionConfig};

use crate::{Client, ClientEvent, SdkError, VoiceEvent, VoiceSession, voice::VoiceSessionParts};

/// The last explicit voice join that may be replayed after a reconnect.
#[derive(Clone)]
pub(crate) struct VoiceIntent {
    /// Voice channel selected by the host.
    pub(crate) channel_id: Snowflake,
    /// Join options selected by the host.
    pub(crate) request: VoiceJoinRequest,
    /// Session identity used to avoid an old cloned handle clearing a newer
    /// session's intent.
    pub(crate) session_id: zephyrvox_types::VoiceSessionId,
}

impl Client {
    /// Joins a voice channel through HTTP and starts its negotiated UDP media
    /// session.
    ///
    /// The operation requires a live control connection.  It selects the
    /// first codec advertised by metadata because v1 has one server-wide
    /// capability set; media payloads remain opaque to the SDK.  Encrypted
    /// session keys are decoded only at this boundary and held in the
    /// zeroizing voice key cache.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::NotConnected`] or [`SdkError::ControlNotLive`]
    /// before making a mutation, a typed HTTP negotiation error, or a voice
    /// transport startup error.
    pub async fn join_voice(
        &self,
        channel_id: Snowflake,
        request: VoiceJoinRequest,
    ) -> Result<VoiceSession, SdkError> {
        self.ensure_open()?;
        let _voice_gate = self.inner.voice_gate.lock().await;
        let voice_intent = request.clone();
        let control = self.current_control().await?;
        if control.status() != ConnectionStatus::Live {
            return Err(SdkError::ControlNotLive);
        }
        let metadata = self.discover().await?;
        let codec = metadata
            .codecs
            .first()
            .cloned()
            .ok_or(SdkError::CodecUnavailable)?;
        // Metadata discovery may overlap a reconnect. Read the generation
        // identity only after that await and immediately before the mutation.
        if control.status() != ConnectionStatus::Live {
            return Err(SdkError::ControlNotLive);
        }
        let control_id = control.id().ok_or(SdkError::VoiceControlUnavailable)?;
        let connection_generation = self.inner.connection_generation.load(Ordering::Acquire);
        let response = self
            .inner
            .api
            .channels()
            .join_voice(
                channel_id,
                request,
                RequestOptions::mutation().with_control_connection(control_id),
            )
            .await?;
        if response.data.channel.id != channel_id {
            return Err(SdkError::channel_mismatch(channel_id));
        }
        let negotiated = &response.data.voice;
        let key = decode_session_key(
            negotiated.encrypted,
            negotiated.created,
            negotiated.key.as_deref(),
        )?;
        // A reused session can be represented by a join response without an
        // inline key. Preserve a clone while replacing a local transport: the
        // old worker removes its session key when it terminates, even when the
        // new worker uses the same server session identity.
        let cached_reuse_key =
            if negotiated.encrypted && !negotiated.created && negotiated.key.is_none() {
                self.inner.key_cache.get(negotiated.session_id)
            } else {
                None
            };
        let config = VoiceSessionConfig::new_with_protocol_version(
            metadata.voice_endpoint.clone(),
            negotiated.session_id,
            negotiated.protocol_version,
            negotiated.encrypted,
            negotiated.max_payload,
            negotiated.expires_at,
            key,
        )?
        .with_stream_types(metadata.voice_stream_types.iter().map(|stream| stream.id))?;
        let transport =
            UdpVoiceSession::connect_with_key_cache(config, self.inner.key_cache.clone()).await?;
        if self.inner.stopped.load(Ordering::Acquire)
            || self.inner.connection_generation.load(Ordering::Acquire) != connection_generation
            || control.status() != ConnectionStatus::Live
            || control.id() != Some(control_id)
        {
            let _ = transport.close().await;
            return Err(if self.inner.stopped.load(Ordering::Acquire) {
                SdkError::Closed
            } else {
                SdkError::VoiceControlUnavailable
            });
        }
        let session = VoiceSession::new(VoiceSessionParts {
            channel_id,
            codec,
            transport,
            api: self.inner.api.clone(),
            control,
            control_generation: connection_generation,
            voice_intent: Arc::clone(&self.inner.voice_intent),
            key_cache: self.inner.key_cache.clone(),
        });
        let previous = self.inner.voice.lock().await.replace(session.clone());
        if let Some(previous) = previous {
            let _ = previous.close_transport().await;
        }
        if let Some(key) = cached_reuse_key {
            self.inner.key_cache.insert(session.session_id(), key);
        }
        if let Ok(mut intent) = self.inner.voice_intent.lock() {
            *intent = self.auto_rejoin_voice().then_some(VoiceIntent {
                channel_id,
                request: voice_intent,
                session_id: session.session_id(),
            });
        }
        let _ = self
            .inner
            .event_tx
            .send(ClientEvent::Voice(VoiceEvent::Joined {
                channel_id,
                session_id: session.session_id(),
            }));
        Ok(session)
    }

    /// Leaves the facade's current voice session, if any.
    ///
    /// # Errors
    ///
    /// Returns the HTTP, control-authority, or UDP teardown error from the
    /// session.  The local UDP worker is stopped even when the HTTP leave
    /// mutation fails.
    pub async fn leave_voice(&self) -> Result<(), SdkError> {
        self.ensure_open()?;
        self.clear_voice_intent();
        let _voice_gate = self.inner.voice_gate.lock().await;
        let session = self.inner.voice.lock().await.take();
        match session {
            Some(session) => {
                let result = session.leave().await;
                if result.is_err() {
                    // Preserve a closed-but-retryable handle when the HTTP
                    // mutation failed after local UDP teardown.  This lets a
                    // host retry an uncertain leave without creating a new
                    // voice session or losing the old session identifier.
                    *self.inner.voice.lock().await = Some(session);
                }
                result
            }
            None => Ok(()),
        }
    }
}

impl Client {
    /// Removes a remembered voice join after authority loss or explicit leave.
    pub(crate) fn clear_voice_intent(&self) {
        if let Ok(mut intent) = self.inner.voice_intent.lock() {
            *intent = None;
        }
    }

    /// Rejoins the remembered voice channel after an opted-in reconnect.
    pub(crate) async fn rejoin_voice_if_configured(&self) -> Result<(), SdkError> {
        if !self.auto_rejoin_voice() {
            return Ok(());
        }
        let intent = self
            .inner
            .voice_intent
            .lock()
            .ok()
            .and_then(|intent| intent.clone());
        let Some(intent) = intent else {
            return Ok(());
        };
        if let Err(error) = self.join_voice(intent.channel_id, intent.request).await {
            self.clear_voice_intent();
            return Err(error);
        }
        Ok(())
    }
}

/// Decodes exactly one encrypted join key without revealing its value.
fn decode_session_key(
    encrypted: bool,
    created: bool,
    key: Option<&str>,
) -> Result<Option<SessionKey>, SdkError> {
    match (encrypted, created, key) {
        (false, _, None) => Ok(None),
        (false, _, Some(_)) => Err(SdkError::negotiation(
            "plaintext voice negotiation included a session key",
        )),
        (true, true, Some(key)) => {
            let bytes = STANDARD
                .decode(key)
                .map_err(|_| SdkError::negotiation("voice session key is not valid base64"))?;
            Ok(Some(SessionKey::from_slice(&bytes)?))
        }
        (true, true, None) => Err(SdkError::negotiation(
            "new encrypted voice negotiation omitted its session key",
        )),
        (true, false, None) => Ok(None),
        (true, false, Some(_)) => Err(SdkError::negotiation(
            "reused encrypted voice negotiation included a session key",
        )),
    }
}
