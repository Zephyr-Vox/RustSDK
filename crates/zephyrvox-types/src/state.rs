use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ControlConnectionId, Cursor, Geid, SessionId, Snowflake, StreamEpoch};

/// Server-global presence activity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceActivity {
    /// Application-defined activity kind.
    #[serde(rename = "type")]
    pub kind: String,
    /// Human-readable activity name.
    pub name: String,
    /// Privacy mode selected by the user.
    pub privacy: String,
}

/// A server-global online status and optional activity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presence {
    /// online, dnd, afk, offline, or invisible.
    pub status: String,
    /// Optional activity after privacy projection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity: Option<PresenceActivity>,
}

/// A credential-free user identity in the visible state projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    /// User Snowflake.
    pub id: Snowflake,
    /// Stable account name.
    pub username: String,
    /// Display nickname.
    pub nickname: String,
    /// Optional avatar reference.
    pub avatar: Option<String>,
}

/// A user identity together with privacy-projected presence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserPresence {
    /// User Snowflake.
    pub user_id: Snowflake,
    /// Display nickname.
    pub nickname: String,
    /// Optional avatar reference.
    pub avatar: Option<String>,
    /// Privacy-projected presence.
    pub presence: Presence,
}

/// A server-scoped role definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Role {
    /// Immutable role key.
    pub key: String,
    /// Editable display name.
    pub display_name: String,
    /// Stable ordering rank.
    pub rank: i64,
    /// Whether this is one of the built-in roles.
    pub builtin: bool,
}

/// A visible channel group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Group {
    /// Group Snowflake.
    pub id: Snowflake,
    /// Display name.
    pub name: String,
    /// Stable UI ordering.
    pub position: i64,
    /// Visibility policy label.
    pub visibility: String,
    /// Entity version.
    pub version: String,
}

/// A visible text, announcement, or voice channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Channel {
    /// Channel Snowflake.
    pub id: Snowflake,
    /// Optional parent group.
    pub group_id: Option<Snowflake>,
    /// Display name.
    pub name: String,
    /// Channel mode.
    pub mode: String,
    /// Whether the voice channel is temporary.
    pub temporary: bool,
    /// Visibility policy label.
    pub visibility: String,
    /// Voice capacity, or the server-provided value for non-voice channels.
    pub capacity: i64,
    /// Stable UI ordering.
    pub position: i64,
    /// Whether the channel is pinned.
    pub pinned: bool,
    /// Entity version.
    pub version: String,
}

/// One visible active voice membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceMembership {
    /// Member user Snowflake.
    pub user_id: Snowflake,
    /// Voice channel Snowflake.
    pub channel_id: Snowflake,
    /// Join time in Unix milliseconds.
    pub joined_at: i64,
}

/// The current user's ephemeral voice authority projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceAuthority {
    /// Voice channel Snowflake.
    pub channel_id: Snowflake,
    /// Control connection that owns this voice binding.
    pub control_connection_id: ControlConnectionId,
    /// UDP voice session identifier.
    pub voice_session_id: SessionId,
    /// Generation used to reject stale teardown callbacks.
    pub voice_authority_generation: String,
}

/// Server-level state metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerState {
    /// Community display name.
    pub name: String,
    /// Backend application version.
    pub version: String,
}

/// The current user's complete visible state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelfUserState {
    /// The current user's identity.
    pub user: User,
    /// Server role keys assigned to the current user.
    pub server_role_keys: Vec<String>,
    /// Effective server permissions.
    pub server_permissions: Vec<String>,
    /// Current user's presence.
    pub presence: Presence,
    /// Current user's voice authority, if any.
    pub voice_authority: Option<VoiceAuthority>,
}

/// Backward-compatible short name for the current-user projection.
pub type SelfState = SelfUserState;

/// Replace-style state body returned by the snapshot endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotState {
    /// Server metadata.
    pub server: ServerState,
    /// Current-user projection.
    #[serde(rename = "self")]
    pub self_state: SelfUserState,
    /// Visible user presence projections.
    pub users: Vec<UserPresence>,
    /// Visible role definitions.
    pub roles: Vec<Role>,
    /// Visible groups.
    pub groups: Vec<Group>,
    /// Visible channels.
    pub channels: Vec<Channel>,
    /// Visible active voice memberships.
    pub voice_memberships: Vec<VoiceMembership>,
}

/// A complete authenticated state snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSnapshot {
    /// Authenticated cursor at this exact snapshot checkpoint.
    pub cursor: Cursor,
    /// Process stream epoch.
    pub stream_epoch: StreamEpoch,
    /// Highest event included in the snapshot.
    pub geid: Geid,
    /// Snapshot schema/state version.
    pub state_version: String,
    /// Replace-style state body.
    pub state: SnapshotState,
}

/// The mutable client-side projection after applying a snapshot and events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientState {
    /// Cursor after the last applied state event.
    pub cursor: Cursor,
    /// Current process stream epoch.
    pub stream_epoch: StreamEpoch,
    /// Highest applied event.
    pub geid: Geid,
    /// Snapshot schema/state version.
    pub state_version: String,
    /// Server metadata.
    pub server: ServerState,
    /// Current-user projection.
    #[serde(rename = "self")]
    pub self_user: SelfUserState,
    /// Visible user presence projections.
    pub users: Vec<UserPresence>,
    /// Visible role definitions.
    pub roles: Vec<Role>,
    /// Visible groups.
    pub groups: Vec<Group>,
    /// Visible channels.
    pub channels: Vec<Channel>,
    /// Visible active voice memberships.
    pub voice_memberships: Vec<VoiceMembership>,
}

impl ClientState {
    /// Creates the public client projection from an authoritative snapshot.
    pub fn from_snapshot(snapshot: StateSnapshot) -> Self {
        let StateSnapshot {
            cursor,
            stream_epoch,
            geid,
            state_version,
            state:
                SnapshotState {
                    server,
                    self_state,
                    users,
                    roles,
                    groups,
                    channels,
                    voice_memberships,
                },
        } = snapshot;
        Self {
            cursor,
            stream_epoch,
            geid,
            state_version,
            server,
            self_user: self_state,
            users,
            roles,
            groups,
            channels,
            voice_memberships,
        }
    }
}

/// A state event scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventScope {
    /// server, group, or channel.
    #[serde(rename = "type")]
    pub kind: String,
    /// Scope Snowflake when the scope is not server-global.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Snowflake>,
}

/// One replayable state event shared by live and replay delivery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateEvent {
    /// Always state.event for this DTO.
    #[serde(rename = "type")]
    pub frame_type: String,
    /// Event sequence identifier.
    pub geid: Geid,
    /// Recipient-authenticated cursor after this event.
    pub cursor: Cursor,
    /// Event class, currently state.
    pub class: String,
    /// Event visibility scope.
    pub scope: EventScope,
    /// Domain event name.
    pub event_type: String,
    /// Causating command identifier, if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<Geid>,
    /// Server event time in Unix milliseconds.
    pub server_time: i64,
    /// Event-specific replace/upsert/delete payload.
    pub data: Value,
}
