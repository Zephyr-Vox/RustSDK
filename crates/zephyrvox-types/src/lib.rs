//! Shared domain types for the ZephyrVox client SDK.
//!
//! This crate intentionally contains no transport implementation.  Its types
//! are the common vocabulary shared by HTTP, WebSocket, UDP voice, and the
//! high-level client facade.

#![forbid(unsafe_code)]

mod errors;
mod fixed_ids;
mod ids;
mod media;
mod metadata;
mod opaque;
mod state;

pub use errors::IdError;
pub use fixed_ids::{ControlConnectionId, SessionId, StreamEpoch, VoiceSessionId};
pub use ids::{Checkpoint, Cursor, Geid, Snowflake, StreamTypeId};
pub use media::{InboundMedia, MediaFrame, OutboundMedia};
pub use metadata::{CodecCapability, Metadata, VoiceEndpoint, VoiceStreamType};
pub use opaque::Etag;
pub use state::{
    Channel, ClientState, EventScope, Group, Presence, PresenceActivity, Role, SelfState,
    SelfUserState, ServerState, SnapshotState, StateEvent, StateSnapshot, User, UserPresence,
    VoiceAuthority, VoiceMembership,
};
