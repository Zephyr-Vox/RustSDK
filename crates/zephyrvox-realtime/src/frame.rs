use serde::Deserialize;
use serde_json::Value;

use crate::{CommandError, RealtimeError};
use zephyrvox_types::{ControlConnectionId, Cursor, Geid, StateEvent};

/// The first server frame after a successful WebSocket upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ConnectionReady {
    /// Server-assigned owner identifier for this control connection.
    pub control_connection_id: ControlConnectionId,
    /// Unix-millisecond access-lease expiry advertised by the server.
    pub access_expires_at: i64,
}

/// A bounded replay batch sent during synchronization.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SyncReplay {
    /// GEID of the first event in `events`.
    pub from_geid: Geid,
    /// GEID of the last event in `events`.
    pub to_geid: Geid,
    /// Recipient-visible state events in increasing GEID order.
    pub events: Vec<StateEvent>,
}

/// Marks the end of replay and carries the cursor at the replay high-water.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SyncComplete {
    /// Authenticated cursor to use for the next reconnect or snapshot.
    pub cursor: Cursor,
}

/// Tells the client to fetch a complete HTTP snapshot before retrying hello.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SyncRequired {
    /// Stable diagnostic reason such as `ring_miss` or `visibility_changed`.
    pub reason: String,
}

/// The terminal notice sent before the server closes a revoked socket.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AuthRevoked {
    /// Server-selected diagnostic reason.
    pub reason: String,
}

/// An unrecognized non-state server frame retained for forward compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct UnknownFrame {
    /// Wire frame type string.
    pub frame_type: String,
    /// Optional command correlation identifier.
    pub request_id: Option<String>,
    /// Untyped frame data retained without silently treating it as state.
    pub data: Value,
}

/// A parsed server frame with known control/state variants and a raw fallback.
#[derive(Debug, Clone, PartialEq)]
pub enum ServerFrame {
    /// Connection identity and access lease.
    ConnectionReady(ConnectionReady),
    /// Ordered replay batch.
    SyncReplay(SyncReplay),
    /// Replay completion cursor.
    SyncComplete(SyncComplete),
    /// Snapshot replacement request.
    SyncRequired(SyncRequired),
    /// One live ordered state event.
    StateEvent(StateEvent),
    /// Successful request-correlated command result.
    CommandOk(crate::CommandAck),
    /// Failed request-correlated command result.
    CommandError(CommandError),
    /// Terminal authentication revocation notice.
    AuthRevoked(AuthRevoked),
    /// Unknown telemetry or future control frame.
    Unknown(UnknownFrame),
}

#[derive(Debug, Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    frame_type: String,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    data: Value,
}

#[derive(Debug, Deserialize)]
struct CommandOkData {
    command_type: String,
    #[serde(default)]
    command_id: Option<String>,
    #[serde(default)]
    expires_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct CommandErrorData {
    command_type: String,
    code: i64,
    message: String,
    retryable: bool,
}

/// Parses one server text frame and validates its known wire shape.
///
/// Unknown frame types are returned as [`ServerFrame::Unknown`] so future
/// telemetry does not get mistaken for ordered state. Unknown `state.*`
/// frames are rejected by the connection layer because state evolution must
/// force a snapshot rather than silently diverge.
///
/// # Errors
///
/// Returns [`RealtimeError::Json`] for invalid JSON and
/// [`RealtimeError::Protocol`] for a known frame with invalid data.
pub fn parse_server_frame(payload: &str) -> Result<ServerFrame, RealtimeError> {
    let raw: Value =
        serde_json::from_str(payload).map_err(|error| RealtimeError::Json(error.to_string()))?;
    let envelope: Envelope = serde_json::from_value(raw.clone())
        .map_err(|error| RealtimeError::Json(error.to_string()))?;
    match envelope.frame_type.as_str() {
        "connection.ready" => {
            decode(envelope.data, "connection.ready").map(ServerFrame::ConnectionReady)
        }
        "sync.replay" => decode(envelope.data, "sync.replay").map(ServerFrame::SyncReplay),
        "sync.complete" => decode(envelope.data, "sync.complete").map(ServerFrame::SyncComplete),
        "sync.required" => decode(envelope.data, "sync.required").map(ServerFrame::SyncRequired),
        "state.event" => {
            let event: StateEvent = decode(raw, "state.event")?;
            Ok(ServerFrame::StateEvent(event))
        }
        "command.ok" => {
            let request_id = required_request_id(envelope.request_id, "command.ok")?;
            let data: CommandOkData = decode(envelope.data, "command.ok")?;
            Ok(ServerFrame::CommandOk(crate::CommandAck {
                request_id,
                command_type: data.command_type,
                command_id: data.command_id,
                expires_at: data.expires_at,
            }))
        }
        "command.error" => {
            let request_id = required_request_id(envelope.request_id, "command.error")?;
            let data: CommandErrorData = decode(envelope.data, "command.error")?;
            Ok(ServerFrame::CommandError(CommandError {
                request_id,
                command_type: data.command_type,
                code: data.code,
                message: data.message,
                retryable: data.retryable,
            }))
        }
        "auth.revoked" => decode(envelope.data, "auth.revoked").map(ServerFrame::AuthRevoked),
        frame_type => Ok(ServerFrame::Unknown(UnknownFrame {
            frame_type: frame_type.to_owned(),
            request_id: envelope.request_id,
            data: envelope.data,
        })),
    }
}

/// Encodes a `sync.hello` frame with an opaque cursor.
pub(crate) fn encode_sync_hello(cursor: &Cursor) -> Result<String, RealtimeError> {
    encode_json(&serde_json::json!({
        "type": "sync.hello",
        "data": { "cursor": cursor.as_str() }
    }))
}

/// Encodes a request-correlated presence command without changing its payload.
pub(crate) fn encode_presence_set(
    request_id: &str,
    command: &crate::PresenceCommand,
) -> Result<String, RealtimeError> {
    encode_json(&serde_json::json!({
        "type": "presence.set",
        "request_id": request_id,
        "data": command,
    }))
}

/// Encodes a request-correlated access-lease update without exposing a
/// refresh token to the WebSocket transport.
pub(crate) fn encode_auth_update(
    request_id: &str,
    access_token: &crate::AccessToken,
) -> Result<String, RealtimeError> {
    encode_json(&serde_json::json!({
        "type": "auth.update",
        "request_id": request_id,
        "data": { "access_token": access_token.as_str() },
    }))
}

/// Deserializes one known frame body and annotates failures with its type.
fn decode<T: for<'de> Deserialize<'de>>(data: Value, frame_type: &str) -> Result<T, RealtimeError> {
    serde_json::from_value(data)
        .map_err(|error| RealtimeError::Protocol(format!("{frame_type} data is invalid: {error}")))
}

/// Validates the bounded request ID carried by a result-bearing frame.
fn required_request_id(
    request_id: Option<String>,
    frame_type: &str,
) -> Result<String, RealtimeError> {
    let request_id = request_id
        .ok_or_else(|| RealtimeError::Protocol(format!("{frame_type} is missing request_id")))?;
    if !valid_request_id(&request_id) {
        return Err(RealtimeError::Protocol(format!(
            "{frame_type} has an invalid request_id"
        )));
    }
    Ok(request_id)
}

/// Checks the server's bounded request identifier grammar.
fn valid_request_id(request_id: &str) -> bool {
    (16..=64).contains(&request_id.len())
        && request_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// Serializes a client frame while preserving the transport error boundary.
fn encode_json(value: &Value) -> Result<String, RealtimeError> {
    serde_json::to_string(value).map_err(|error| RealtimeError::Json(error.to_string()))
}
