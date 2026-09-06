use serde::{Deserialize, Serialize};

use crate::pagination::query_path;
use crate::{
    ApiClient, ApiResponse, HttpError, HttpMethod, Page, RequestOptions, client::AuthRequirement,
};
use zephyrvox_types::Snowflake;

/// Server, group, or channel scope used by moderation endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModerationScope {
    /// Scope kind: `server`, `group`, or `channel`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Group or channel identifier; omitted for server scope.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Snowflake>,
}

/// Mute resource returned by the moderation control plane.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Mute {
    /// Mute identifier.
    pub id: Snowflake,
    /// Scope where the mute applies.
    pub scope: ModerationScope,
    /// Muted user identifier.
    pub user_id: Snowflake,
    /// Mute kind, normally `text`, `voice`, or `desktop_audio`.
    pub kind: String,
    /// Expiry in Unix milliseconds, or `None` for an indefinite mute.
    pub expires_at: Option<i64>,
    /// Human-readable moderation reason.
    pub reason: String,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
    /// Strong resource version returned by the server.
    pub version: String,
}

/// Request body for creating a mute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateMuteRequest {
    /// Scope where the mute applies.
    pub scope: ModerationScope,
    /// User to mute.
    pub user_id: Snowflake,
    /// Mute kind.
    pub kind: String,
    /// Optional expiry in Unix milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    /// Optional moderation reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Partial mute update. `Some(None)` explicitly clears an expiry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct UpdateMuteRequest {
    /// Expiry update; omitted preserves it and null clears it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<Option<i64>>,
    /// Replacement reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Typed accessor for moderation mute endpoints.
pub struct ModerationApi<'a> {
    client: &'a ApiClient,
}

impl ApiClient {
    /// Returns the moderation resource accessor.
    pub fn moderation(&self) -> ModerationApi<'_> {
        ModerationApi { client: self }
    }
}

impl ModerationApi<'_> {
    /// Lists mutes for one exact scope and optional target filters.
    pub async fn list(
        &self,
        scope: ModerationScope,
        user_id: Option<Snowflake>,
        kind: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Page<Mute>>, HttpError> {
        let mut parameters = vec![
            ("scope_type", scope.kind.clone()),
            ("limit", limit.to_string()),
            ("offset", offset.to_string()),
        ];
        if let Some(scope_id) = scope.id {
            parameters.push(("scope_id", scope_id.to_string()));
        }
        if let Some(user_id) = user_id {
            parameters.push(("user_id", user_id.to_string()));
        }
        if let Some(kind) = kind {
            parameters.push(("kind", kind.to_owned()));
        }
        let path = query_path("mutes", parameters);
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Creates a mute and returns the committed state metadata.
    pub async fn create(
        &self,
        input: CreateMuteRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<Mute>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .json_request(HttpMethod::Post, "mutes", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Updates a mute after the caller supplies its current ETag in options.
    pub async fn update(
        &self,
        mute_id: Snowflake,
        input: UpdateMuteRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<Mute>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("mutes/{mute_id}");
        let request = self
            .client
            .json_request(HttpMethod::Patch, &path, &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Deletes a mute after the caller supplies its current ETag in options.
    pub async fn delete(
        &self,
        mute_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("mutes/{mute_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}
