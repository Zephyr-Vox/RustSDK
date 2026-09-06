use std::{
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use serde::Serialize;

use crate::{
    ApiClient, ApiResponse, HttpError, HttpMethod, RequestOptions, UserDetail,
    client::AuthRequirement, pagination::page_path,
};
use zephyrvox_types::{Snowflake, User};

/// Partial profile update for the current user or an administrator-managed user.
#[derive(Clone, Serialize, Default)]
pub struct UpdateUserRequest {
    /// Replacement nickname. `None` leaves the nickname unchanged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nickname: Option<String>,
}

impl fmt::Debug for UpdateUserRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpdateUserRequest")
            .field("nickname", &self.nickname)
            .finish()
    }
}

/// Password replacement request for an administrator-managed user.
#[derive(Clone, Serialize)]
pub struct ResetPasswordRequest {
    /// New account password.
    pub password: String,
}

impl fmt::Debug for ResetPasswordRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResetPasswordRequest")
            .field("password", &"<redacted>")
            .finish()
    }
}

/// Current-user password change request.
#[derive(Clone, Serialize)]
pub struct ChangePasswordRequest {
    /// Current account password.
    pub old_password: String,
    /// Replacement account password.
    pub new_password: String,
}

impl fmt::Debug for ChangePasswordRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangePasswordRequest")
            .field("old_password", &"<redacted>")
            .field("new_password", &"<redacted>")
            .finish()
    }
}

/// Response returned after an avatar upload.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct AvatarUploadResponse {
    /// Stored avatar object name, without the public `/avatar/` prefix.
    pub avatar: String,
}

/// Typed accessor for user administration and self-service profile endpoints.
pub struct UserApi<'a> {
    client: &'a ApiClient,
}

impl ApiClient {
    /// Returns the user and profile resource accessor.
    pub fn users(&self) -> UserApi<'_> {
        UserApi { client: self }
    }
}

impl UserApi<'_> {
    /// Lists detailed users using the server's bounded pagination contract.
    pub async fn list(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Vec<UserDetail>>, HttpError> {
        let path = page_path("users", limit, offset);
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Fetches one detailed user record.
    pub async fn get(&self, user_id: Snowflake) -> Result<ApiResponse<UserDetail>, HttpError> {
        let path = format!("users/{user_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Updates an administrator-managed user's nickname.
    pub async fn update(
        &self,
        user_id: Snowflake,
        input: UpdateUserRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<User>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("users/{user_id}");
        let request = self
            .client
            .json_request(HttpMethod::Patch, &path, &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Resets an administrator-managed user's password.
    pub async fn reset_password(
        &self,
        user_id: Snowflake,
        input: ResetPasswordRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("users/{user_id}/password");
        let request = self
            .client
            .json_request(HttpMethod::Post, &path, &input, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Invalidates a user's sessions and active control connections.
    pub async fn kick(
        &self,
        user_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        self.no_body_action(user_id, "kick", options).await
    }

    /// Bans a user and invalidates the user's active sessions.
    pub async fn ban(
        &self,
        user_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        self.no_body_action(user_id, "ban", options).await
    }

    /// Removes a user's ban.
    pub async fn unban(
        &self,
        user_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        self.no_body_action(user_id, "unban", options).await
    }

    /// Permanently deletes a user account.
    pub async fn delete(
        &self,
        user_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("users/{user_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Updates the current user's nickname.
    pub async fn update_profile(
        &self,
        input: UpdateUserRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<User>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .json_request(HttpMethod::Patch, "me", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Changes the current user's password.
    pub async fn change_password(
        &self,
        input: ChangePasswordRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .json_request(HttpMethod::Post, "me/password", &input, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Uploads an avatar as the server's required `file` multipart field.
    pub async fn upload_avatar(
        &self,
        filename: &str,
        content_type: &str,
        bytes: Bytes,
        options: RequestOptions,
    ) -> Result<ApiResponse<AvatarUploadResponse>, HttpError> {
        let boundary = multipart_boundary();
        let body = multipart_body(&boundary, filename, content_type, bytes);
        let mut options = options;
        options.ensure_idempotency();
        let mut request = self
            .client
            .empty_request(HttpMethod::Post, "me/avatar", options)?;
        request.set_header(
            "content-type",
            &format!("multipart/form-data; boundary={boundary}"),
        )?;
        request.set_body(body);
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Deletes the current user's avatar.
    pub async fn delete_avatar(
        &self,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .empty_request(HttpMethod::Delete, "me/avatar", options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    async fn no_body_action(
        &self,
        user_id: Snowflake,
        action: &str,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("users/{user_id}/{action}");
        let request = self
            .client
            .empty_request(HttpMethod::Post, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}

fn multipart_boundary() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("zephyrvox-{nanos:x}")
}

fn multipart_body(boundary: &str, filename: &str, content_type: &str, bytes: Bytes) -> Bytes {
    let safe_filename = header_value(filename, "upload.bin");
    let safe_content_type = header_value(content_type, "application/octet-stream");
    let mut body = Vec::with_capacity(bytes.len() + 256);
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{safe_filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(format!("Content-Type: {safe_content_type}\r\n\r\n").as_bytes());
    body.extend_from_slice(&bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    Bytes::from(body)
}

fn header_value(value: &str, fallback: &str) -> String {
    let filtered: String = value
        .chars()
        .filter(|character| !character.is_ascii_control() && *character != '"')
        .collect();
    if filtered.is_empty() {
        fallback.to_owned()
    } else {
        filtered
    }
}
