//! High-level Tokio-first client facade for ZephyrVox CommunityServer.
//!
//! [`Client`] owns the HTTP, WebSocket, and UDP voice coordination boundaries.
//! The lower-level domain APIs remain available through the [`http`] module
//! for hosts that need a typed endpoint not yet represented by the facade.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod adapters;
mod client;
mod config;
mod errors;
mod events;
mod lifecycle;
mod provider;
mod voice;
mod voice_api;

/// Typed HTTP domain APIs re-exported for hosts that depend only on this
/// facade crate.
pub mod http {
    pub use zephyrvox_http::*;
}

pub use adapters::{AudioCodec, BoxFuture, MediaError, MediaSink, MediaSource, PcmFrame};
pub use client::Client;
pub use config::ClientConfig;
pub use errors::SdkError;
pub use events::{ClientEvent, EventStream, VoiceEvent};
pub use voice::VoiceSession;

pub use zephyrvox_http::{
    AccessToken as HttpAccessToken, ActivateRequest, AdminApi, AdminMetrics, ApiClient,
    ApiResponse, AuthApi, AuthStatus, AvatarUploadResponse, ChangePasswordRequest, ChannelApi,
    CreateBindingRequest, CreateChannelRequest, CreateGroupRequest, CreateInviteRequest,
    CreateInviteResponse, CreateMuteRequest, CreateRoleRequest, CredentialStore,
    CredentialStoreError, GroupApi, HttpError, HttpMethod, HttpRequest, HttpResponse,
    HttpTransport, Invite, LoadHardLimits, LoadInputMetrics, LoadMetrics, LoadSoftLimits,
    LoginRequest, LoginResult, MeResult, MemoryCredentialStore, ModerationApi, ModerationScope,
    OwnerTransferResponse, Page, PermissionConfig, QueueMetrics, RbacApi, RbacScope, RefreshToken,
    RegisterRequest, RegisterResult, RelayMetrics, RequestOptions, ReqwestTransport,
    ResetPasswordRequest, ResponseHeaders, RoleBinding, RoleRecord, TokenPair, UdpMetrics,
    UpdateChannelRequest, UpdateGroupRequest, UpdatePermissionConfigRequest, UpdateRoleRequest,
    UpdateUserRequest, UserApi, UserDetail, VoiceJoinRequest, VoiceJoinResponse, VoiceLeaveRequest,
    VoiceNegotiation,
};

pub use zephyrvox_realtime::{
    AccessToken, CommandAck, CommandError, ConnectionStatus, ControlConnection, RealtimeConfig,
    RealtimeError, ReconnectPolicy, SnapshotProvider, StateApplyError, StateEventReceiver,
    StateStore, SyncError, SyncMachine, SyncPhase, TokioTungsteniteConnector, WebSocketConnector,
    WebSocketMessage, WebSocketSession,
};

pub use zephyrvox_types::{
    Channel, Checkpoint, ClientState, CodecCapability, ControlConnectionId, Cursor, Etag,
    EventScope, Geid, Group, InboundMedia, MediaFrame, Metadata, OutboundMedia, Presence,
    PresenceActivity, Role, SelfState, SelfUserState, ServerState, SnapshotState, Snowflake,
    StateEvent, StateSnapshot, StreamEpoch, StreamTypeId, User, UserPresence, VoiceAuthority,
    VoiceEndpoint, VoiceMembership, VoiceSessionId, VoiceStreamType,
};

pub use zephyrvox_voice::{
    InboundMedia as VoiceInboundMedia, PacketCodec, PacketError, RevocationReason, SessionKey,
    SessionKeyCache, VoiceError, VoiceSessionConfig, VoiceStatus,
};

pub use zephyrvox_wire::{
    Fingerprint, PROTOCOL_VERSION, ServerCard, ServerCardError, TransportScheme,
};
