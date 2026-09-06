use std::fmt;

use serde::{Deserialize, Serialize};
use zephyrvox_types::User;

use crate::TokenPair;

/// Login request credentials.
#[derive(Clone, Serialize)]
pub struct LoginRequest {
    /// Account username.
    pub username: String,
    /// Account password.
    pub password: String,
    /// Client/device identity used for session tracking.
    pub device_id: String,
}

impl fmt::Debug for LoginRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginRequest")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("device_id", &self.device_id)
            .finish()
    }
}

/// New account registration request.
#[derive(Clone, Serialize)]
pub struct RegisterRequest {
    /// Desired account username.
    pub username: String,
    /// Desired account password.
    pub password: String,
    /// Optional display nickname.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,
    /// Optional invite code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invite: Option<String>,
}

impl fmt::Debug for RegisterRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisterRequest")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("nickname", &self.nickname)
            .field("invite", &self.invite.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// First-owner activation request.
#[derive(Clone, Serialize)]
pub struct ActivateRequest {
    /// One-time activation code.
    pub code: String,
    /// Owner account username.
    pub username: String,
    /// Owner account password.
    pub password: String,
    /// Optional owner nickname.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,
}

impl fmt::Debug for ActivateRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActivateRequest")
            .field("code", &"<redacted>")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("nickname", &self.nickname)
            .finish()
    }
}

/// Public registration/bootstrap status.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct AuthStatus {
    /// open or invite registration mode.
    pub registration_mode: String,
    /// Whether first-owner activation is still required.
    pub activation_required: bool,
}

/// Successful login result.
#[derive(Clone)]
pub struct LoginResult {
    /// Rotated access and refresh credentials.
    pub tokens: TokenPair,
    /// Authenticated user profile.
    pub user: User,
}

impl fmt::Debug for LoginResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginResult")
            .field("tokens", &self.tokens)
            .field("user", &self.user)
            .finish()
    }
}

/// Successful registration result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterResult {
    /// Newly created user.
    pub user: User,
}

/// Authenticated profile and effective permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeResult {
    /// Current user profile.
    pub user: User,
    /// Effective permission strings.
    pub permissions: Vec<String>,
}

/// A detailed user record returned by administrative user endpoints.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct UserDetail {
    /// User identity.
    pub id: zephyrvox_types::Snowflake,
    /// Username.
    pub username: String,
    /// Nickname.
    pub nickname: String,
    /// Optional avatar.
    #[serde(deserialize_with = "deserialize_optional_avatar")]
    pub avatar: Option<String>,
    /// Assigned role keys.
    pub roles: Vec<String>,
    /// Whether the user is banned.
    pub banned: bool,
    /// Last login timestamp.
    pub last_login_at: Option<i64>,
    /// Account creation timestamp.
    pub created_at: i64,
}

fn deserialize_optional_avatar<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let avatar = Option::<String>::deserialize(deserializer)?;
    Ok(avatar.filter(|avatar| !avatar.is_empty()))
}
