//! Errors exposed by the high-level SDK facade.

use thiserror::Error;

use zephyrvox_http::{CredentialStoreError, HttpError};
use zephyrvox_realtime::RealtimeError;
use zephyrvox_types::{IdError, Snowflake};
use zephyrvox_voice::VoiceError;
use zephyrvox_wire::ServerCardError;

/// Errors returned by the high-level client facade.
#[derive(Debug, Error)]
pub enum SdkError {
    /// The HTTP control-plane operation failed.
    #[error(transparent)]
    Http(#[from] HttpError),
    /// The WebSocket control connection failed.
    #[error(transparent)]
    Realtime(#[from] RealtimeError),
    /// The UDP media session failed.
    #[error(transparent)]
    Voice(#[from] VoiceError),
    /// The credential store could not complete an operation.
    #[error(transparent)]
    Credentials(#[from] CredentialStoreError),
    /// The server card or another shared wire value was invalid.
    #[error(transparent)]
    ServerCard(#[from] ServerCardError),
    /// A shared identifier could not be constructed or decoded.
    #[error(transparent)]
    Identifier(#[from] IdError),
    /// The client configuration is internally inconsistent.
    #[error("invalid client configuration: {0}")]
    Configuration(String),
    /// The operation requires a control connection that has not been opened.
    #[error("no active control connection")]
    NotConnected,
    /// The control connection exists but has not reached the live phase.
    #[error("control connection is not live")]
    ControlNotLive,
    /// The current voice control identity is unavailable.
    #[error("voice control authority is unavailable")]
    VoiceControlUnavailable,
    /// The server did not advertise a codec that can describe the voice
    /// session.
    #[error("server metadata does not advertise a voice codec")]
    CodecUnavailable,
    /// The HTTP join response contained a negotiation value that cannot be
    /// passed safely to the UDP transport.
    #[error("invalid voice negotiation: {0}")]
    VoiceNegotiation(String),
    /// The facade has already been shut down.
    #[error("SDK client is closed")]
    Closed,
}

impl SdkError {
    /// Creates a redacted configuration error for a value that cannot be
    /// represented safely in the facade's public lifecycle.
    pub(crate) fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration(message.into())
    }

    /// Creates a negotiation error without including key material or tokens.
    pub(crate) fn negotiation(message: impl Into<String>) -> Self {
        Self::VoiceNegotiation(message.into())
    }

    /// Creates a stable error for a voice channel identifier mismatch.
    pub(crate) fn channel_mismatch(channel_id: Snowflake) -> Self {
        Self::negotiation(format!("join response does not match channel {channel_id}"))
    }
}
