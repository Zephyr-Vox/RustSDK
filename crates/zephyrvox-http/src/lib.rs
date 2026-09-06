//! Tokio-first HTTP control-plane implementation for CommunityServer.
//!
//! [`ApiClient`] is the main entry point.  It keeps the server-card transport
//! decision fixed, exposes typed domain accessors, and coordinates credential
//! loading and refresh-token rotation across concurrent requests.  The lower
//! level [`HttpTransport`] and [`HttpRequest`] types are public so hosts can
//! supply deterministic fakes or an instrumented adapter without coupling the
//! endpoint modules to a concrete HTTP runtime.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod admin;
mod auth;
mod auth_types;
mod channels;
mod client;
mod credentials;
mod discovery;
mod errors;
mod future;
mod groups;
mod moderation;
mod pagination;
mod rbac;
mod request;
mod response;
mod tls_pin;
mod transport;
mod users;

pub use admin::{
    AdminApi, AdminMetrics, CreateInviteRequest, CreateInviteResponse, Invite, LoadHardLimits,
    LoadInputMetrics, LoadMetrics, LoadSoftLimits, QueueMetrics, RelayMetrics, UdpMetrics,
};
pub use auth::AuthApi;
pub use auth_types::{
    ActivateRequest, AuthStatus, LoginRequest, LoginResult, MeResult, RegisterRequest,
    RegisterResult, UserDetail,
};
pub use channels::{
    ChannelApi, CreateChannelRequest, UpdateChannelRequest, VoiceJoinRequest, VoiceJoinResponse,
    VoiceLeaveRequest, VoiceNegotiation,
};
pub use client::ApiClient;
pub use credentials::{
    AccessToken, CredentialStore, MemoryCredentialStore, RefreshToken, TokenPair,
};
pub use errors::{CredentialStoreError, HttpError};
pub use future::BoxFuture;
pub use groups::{AccessEntry, AccessPrincipal, CreateGroupRequest, GroupApi, UpdateGroupRequest};
pub use moderation::{CreateMuteRequest, ModerationApi, ModerationScope, Mute, UpdateMuteRequest};
pub use pagination::Page;
pub use rbac::{
    CreateBindingRequest, CreateRoleRequest, OwnerTransferResponse, PermissionConfig, RbacApi,
    RbacScope, RoleBinding, RoleRecord, UpdatePermissionConfigRequest, UpdateRoleRequest,
};
pub use request::{HttpMethod, HttpRequest, HttpResponse, RequestOptions};
pub use response::{ApiResponse, ResponseHeaders};
pub use transport::{HttpTransport, ReqwestTransport};
pub use users::{
    AvatarUploadResponse, ChangePasswordRequest, ResetPasswordRequest, UpdateUserRequest, UserApi,
};
