use std::fmt;

use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

use super::ChannelApi;
use crate::{ApiResponse, HttpError, HttpMethod, RequestOptions, client::AuthRequirement};
use zephyrvox_types::{Channel, Checkpoint, Cursor, Snowflake, VoiceMembership, VoiceSessionId};
use zephyrvox_wire::{PROTOCOL_VERSION, max_payload};

/// Request body for joining a voice channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct VoiceJoinRequest {
    /// Optional client/device identity used for session tracking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    /// Requests replacement of the current voice session.
    pub force_new: bool,
    /// Expected current session for stale-session protection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_voice_session_id: Option<VoiceSessionId>,
}

/// Request body for leaving the current voice channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VoiceLeaveRequest {
    /// Session that the caller believes it owns.
    pub voice_session_id: VoiceSessionId,
}

/// HTTP voice negotiation data returned by a successful join.
#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct VoiceNegotiation {
    /// Whether a new UDP session was allocated.
    pub created: bool,
    /// UDP session identifier.
    pub session_id: VoiceSessionId,
    /// Base64-encoded session key when a new encrypted session was created.
    pub key: Option<String>,
    /// Whether UDP payloads are encrypted.
    pub encrypted: bool,
    /// Server warning, for example `plaintext_mode`.
    pub warning: Option<String>,
    /// Maximum UDP payload accepted by the server.
    pub max_payload: usize,
    /// Protocol version used by the voice transport.
    pub protocol_version: u16,
    /// Session expiry in Unix milliseconds.
    pub expires_at: i64,
}

impl fmt::Debug for VoiceNegotiation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VoiceNegotiation")
            .field("created", &self.created)
            .field("session_id", &self.session_id)
            .field("key", &self.key.as_ref().map(|_| "<redacted>"))
            .field("encrypted", &self.encrypted)
            .field("warning", &self.warning)
            .field("max_payload", &self.max_payload)
            .field("protocol_version", &self.protocol_version)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// HTTP voice join response, including the state checkpoint to apply.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct VoiceJoinResponse {
    /// Joined voice channel projection.
    pub channel: Channel,
    /// Members visible at the returned checkpoint.
    pub members: Vec<VoiceMembership>,
    /// UDP session negotiation data.
    pub voice: VoiceNegotiation,
    /// Authenticated state cursor after the join.
    pub state_cursor: Cursor,
    /// Stream checkpoint after the join.
    pub state_checkpoint: Checkpoint,
}

impl ChannelApi<'_> {
    /// Joins a voice channel using an active control connection.
    pub async fn join_voice(
        &self,
        channel_id: Snowflake,
        input: VoiceJoinRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<VoiceJoinResponse>, HttpError> {
        if !options.has_control_connection() {
            return Err(HttpError::MissingControlConnection);
        }
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("channels/{channel_id}/join");
        let request = self
            .client
            .json_request(HttpMethod::Post, &path, &input, options)?;
        let response = self
            .client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await?;
        validate_voice_join(
            channel_id,
            &response.data,
            self.client.server_card().scheme().is_tls(),
        )?;
        Ok(response)
    }

    /// Leaves the current voice channel using the active control connection.
    pub async fn leave_voice(
        &self,
        input: VoiceLeaveRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        if !options.has_control_connection() {
            return Err(HttpError::MissingControlConnection);
        }
        let mut options = options;
        options.ensure_idempotency();
        let request = self.client.json_request(
            HttpMethod::Post,
            "channels/current/leave",
            &input,
            options,
        )?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}

/// Validates negotiated voice values before handing them to a UDP client.
fn validate_voice_join(
    requested_channel_id: Snowflake,
    response: &VoiceJoinResponse,
    tls_card: bool,
) -> Result<(), HttpError> {
    if response.channel.id != requested_channel_id {
        return Err(HttpError::Wire(
            "voice join response channel does not match the requested channel".to_owned(),
        ));
    }
    if response.channel.mode != "voice" {
        return Err(HttpError::Wire(
            "voice join response does not contain a voice channel".to_owned(),
        ));
    }
    let voice = &response.voice;
    let expected_version = u16::from(PROTOCOL_VERSION);
    if voice.protocol_version != expected_version {
        return Err(HttpError::ProtocolVersionMismatch {
            expected: expected_version,
            actual: voice.protocol_version,
        });
    }
    if tls_card && !voice.encrypted {
        return Err(HttpError::Wire(
            "plaintext voice sessions are not allowed for TLS server cards".to_owned(),
        ));
    }
    if voice.max_payload == 0 || voice.max_payload > max_payload(voice.encrypted) {
        return Err(HttpError::Wire(
            "voice join response exceeds the protocol payload limit".to_owned(),
        ));
    }
    if voice.expires_at <= 0 {
        return Err(HttpError::Wire(
            "voice join response has an invalid session expiry".to_owned(),
        ));
    }
    if response
        .members
        .iter()
        .any(|member| member.channel_id != requested_channel_id)
    {
        return Err(HttpError::Wire(
            "voice join response contains a member from another channel".to_owned(),
        ));
    }
    if voice.encrypted && voice.created {
        let valid_key = voice.key.as_deref().is_some_and(|key| {
            STANDARD
                .decode(key)
                .is_ok_and(|decoded| decoded.len() == 32)
        });
        if !valid_key {
            return Err(HttpError::Wire(
                "new encrypted voice sessions must include a 32-byte base64 session key".to_owned(),
            ));
        }
    }
    if voice.encrypted && !voice.created && voice.key.is_some() {
        return Err(HttpError::Wire(
            "reused encrypted voice sessions must not include a session key".to_owned(),
        ));
    }
    if !voice.encrypted && voice.key.is_some() {
        return Err(HttpError::Wire(
            "plaintext voice sessions must not include a session key".to_owned(),
        ));
    }
    Ok(())
}
