use serde::Serialize;

use zephyrvox_types::Presence;

/// A successful command acknowledgement returned by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandAck {
    /// Request identifier supplied by the SDK.
    pub request_id: String,
    /// Command name acknowledged by the server.
    pub command_type: String,
    /// Stable command identifier for commands that also publish state events.
    pub command_id: Option<String>,
    /// Refreshed access-lease expiry for `auth.update`, when present.
    pub expires_at: Option<i64>,
}

/// The supported connection-local presence command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PresenceCommand {
    /// The server-global status requested by the user.
    pub status: String,
    /// Optional activity to publish with the status.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity: Option<zephyrvox_types::PresenceActivity>,
}

impl From<Presence> for PresenceCommand {
    fn from(presence: Presence) -> Self {
        Self {
            status: presence.status,
            activity: presence.activity,
        }
    }
}
