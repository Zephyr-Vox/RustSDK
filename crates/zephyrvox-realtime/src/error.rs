use thiserror::Error;

use zephyrvox_types::{Geid, StreamEpoch};

/// Errors returned by the realtime control connection.
#[derive(Debug, Error)]
pub enum RealtimeError {
    /// The injected snapshot or credential provider failed.
    ///
    /// Provider implementations must sanitize the detail and never include
    /// access tokens, refresh tokens, or voice keys.
    #[error("realtime provider failed: {0}")]
    Provider(String),
    /// The provider has no authenticated access token.
    #[error("authentication is required for the realtime connection")]
    AuthenticationRequired,
    /// The provider's refresh token was rejected or the session expired.
    #[error("realtime authentication expired")]
    AuthenticationExpired,
    /// The WebSocket transport could not connect, send, or receive.
    #[error("WebSocket transport failed: {0}")]
    Transport(String),
    /// The server frame was not valid for the negotiated protocol.
    #[error("WebSocket protocol violation: {0}")]
    Protocol(String),
    /// A frame body was not valid JSON for its declared frame type.
    #[error("invalid WebSocket JSON: {0}")]
    Json(String),
    /// The server sent a state event that could not be applied atomically.
    #[error(transparent)]
    State(#[from] StateApplyError),
    /// The synchronizer rejected an ordering or lifecycle transition.
    #[error(transparent)]
    Sync(#[from] SyncError),
    /// The server returned a correlated command failure.
    #[error(transparent)]
    ServerCommand(#[from] CommandError),
    /// A command could not be admitted because the connection is not live.
    #[error("WebSocket control connection is not live")]
    NotLive,
    /// A command response did not arrive before its configured deadline.
    #[error("WebSocket command timed out")]
    CommandTimeout,
    /// The connection was deliberately closed by the caller or terminal peer.
    #[error("WebSocket control connection is closed")]
    Closed,
    /// The connection was stopped because the server explicitly revoked it.
    #[error("WebSocket connection was revoked: {0}")]
    Revoked(String),
    /// The TLS peer's SPKI did not match the server card.
    #[error("TLS SPKI fingerprint mismatch: expected {expected}, got {actual}")]
    TlsPinMismatch {
        /// Fingerprint carried by the server card.
        expected: String,
        /// Fingerprint computed from the peer certificate.
        actual: String,
    },
    /// The TLS peer certificate could not be parsed or verified.
    #[error("TLS peer certificate validation failed")]
    TlsCertificateInvalid,
    /// The reconnect policy has no remaining attempts.
    #[error("WebSocket reconnect attempts exhausted")]
    ReconnectExhausted,
    /// The configured endpoint or limits are internally inconsistent.
    #[error("invalid realtime configuration: {0}")]
    Configuration(String),
}

/// The structured failure returned by a server-side command.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("command {command_type} failed with code {code}: {message}")]
pub struct CommandError {
    /// Request identifier used to correlate the response.
    pub request_id: String,
    /// Server command name, such as `presence.set`.
    pub command_type: String,
    /// Endpoint-local or command-local error code.
    pub code: i64,
    /// Human-readable server explanation.
    pub message: String,
    /// Whether retrying the same logical operation may succeed.
    pub retryable: bool,
}

/// Errors raised while applying one ordered state event to [`crate::StateStore`].
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StateApplyError {
    /// The event envelope did not identify a state event.
    #[error("invalid state event envelope: {0}")]
    InvalidEnvelope(String),
    /// The event's GEID was not greater than the last applied GEID.
    #[error("state GEID {actual} is not after current GEID {current}")]
    GeidOrder {
        /// Last GEID installed in the projection.
        current: Geid,
        /// GEID supplied by the rejected event.
        actual: Geid,
    },
    /// The event named a state transition that this SDK version does not know.
    #[error("unknown state event type: {0}")]
    UnknownEvent(String),
    /// The event payload did not match the known event's replacement shape.
    #[error("invalid payload for state event {event_type}: {message}")]
    InvalidPayload {
        /// Event name whose payload failed validation.
        event_type: String,
        /// Validation detail suitable for diagnostics.
        message: String,
    },
    /// The event cursor was empty or otherwise invalid.
    #[error("state event cursor is invalid")]
    InvalidCursor,
}

/// Errors raised by the pure synchronization state machine.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SyncError {
    /// The state machine received a frame in a phase where it is not legal.
    #[error("invalid synchronization phase transition: {0}")]
    InvalidPhase(String),
    /// Replay frame bounds did not match the events it contained.
    #[error("sync replay GEID bounds are inconsistent")]
    ReplayBounds,
    /// A replay or live event moved backwards in the ordered stream.
    #[error("state stream GEID {actual} is not after {current}")]
    GeidOrder {
        /// Last accepted event GEID.
        current: Geid,
        /// Rejected event GEID.
        actual: Geid,
    },
    /// A new stream epoch was observed where the current snapshot epoch was
    /// still authoritative.
    #[error("state stream epoch changed from {expected} to {actual}")]
    StreamEpochChanged {
        /// Epoch bound to the current local snapshot.
        expected: StreamEpoch,
        /// Epoch claimed by the incoming synchronization context.
        actual: StreamEpoch,
    },
    /// The server requested a replacement HTTP snapshot.
    #[error("server requires a state snapshot: {0}")]
    SnapshotRequired(String),
}
