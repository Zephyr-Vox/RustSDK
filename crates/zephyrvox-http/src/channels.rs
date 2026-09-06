use serde::Serialize;

use crate::{
    AccessEntry, AccessPrincipal, ApiClient, ApiResponse, HttpError, HttpMethod, Page,
    RequestOptions,
    client::AuthRequirement,
    groups::{access_path, channel_access_request},
    pagination::{page_path, query_path},
};
use zephyrvox_types::{Channel, Snowflake};

mod voice;

pub use voice::{VoiceJoinRequest, VoiceJoinResponse, VoiceLeaveRequest, VoiceNegotiation};

/// Request body for creating a channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateChannelRequest {
    /// Optional parent group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<Snowflake>,
    /// Display name.
    pub name: String,
    /// Channel mode, normally `voice`, `text`, or `announcement`.
    pub mode: String,
    /// Whether the channel is temporary.
    pub temporary: bool,
    /// Visibility policy, normally `public` or `private`.
    pub visibility: String,
    /// Voice capacity, or `None` to use the server default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<i64>,
    /// UI ordering position.
    pub position: i64,
    /// Whether the channel is pinned.
    pub pinned: bool,
}

/// Partial update body for a channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct UpdateChannelRequest {
    /// `None` omits the field; `Some(None)` explicitly removes the group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<Option<Snowflake>>,
    /// Replacement display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Replacement visibility policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    /// Replacement voice capacity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<i64>,
    /// Replacement UI ordering position.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
    /// Replacement pinned state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pinned: Option<bool>,
}

/// Typed accessor for channel, ACL, and voice endpoints.
pub struct ChannelApi<'a> {
    pub(super) client: &'a ApiClient,
}

impl ApiClient {
    /// Returns the channel resource accessor.
    pub fn channels(&self) -> ChannelApi<'_> {
        ChannelApi { client: self }
    }
}

impl ChannelApi<'_> {
    /// Lists visible channels, optionally restricted to a parent group.
    pub async fn list(
        &self,
        group_id: Option<Snowflake>,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Page<Channel>>, HttpError> {
        let path = match group_id {
            Some(group_id) => query_path(
                "channels",
                [
                    ("group_id", group_id.to_string()),
                    ("limit", limit.to_string()),
                    ("offset", offset.to_string()),
                ],
            ),
            None => page_path("channels", limit, offset),
        };
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Fetches one visible channel and its current response ETag.
    pub async fn get(&self, channel_id: Snowflake) -> Result<ApiResponse<Channel>, HttpError> {
        let path = format!("channels/{channel_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Creates a channel and returns the committed state metadata.
    pub async fn create(
        &self,
        input: CreateChannelRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<Channel>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .json_request(HttpMethod::Post, "channels", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Updates a channel after the caller supplies its current ETag in options.
    pub async fn update(
        &self,
        channel_id: Snowflake,
        input: UpdateChannelRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<Channel>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("channels/{channel_id}");
        let request = self
            .client
            .json_request(HttpMethod::Patch, &path, &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Deletes a channel after the caller supplies its current ETag in options.
    pub async fn delete(
        &self,
        channel_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("channels/{channel_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Lists ACL entries for a channel and returns the channel's current ETag.
    pub async fn list_access(
        &self,
        channel_id: Snowflake,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Page<AccessEntry>>, HttpError> {
        let path = access_path(&format!("channels/{channel_id}/access"), limit, offset);
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Adds or reuses a channel ACL entry using its ETag precondition.
    pub async fn add_access(
        &self,
        channel_id: Snowflake,
        principal: AccessPrincipal,
        grant_parent: bool,
        options: RequestOptions,
    ) -> Result<ApiResponse<AccessEntry>, HttpError> {
        options.require_if_match()?;
        if grant_parent {
            options.require_parent_if_match()?;
        }
        let mut options = options;
        options.ensure_idempotency();
        let body = channel_access_request(principal, grant_parent);
        let path = format!("channels/{channel_id}/access");
        let request = self
            .client
            .json_request(HttpMethod::Post, &path, &body, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Removes a channel ACL entry using the channel's ETag precondition.
    pub async fn delete_access(
        &self,
        channel_id: Snowflake,
        access_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("channels/{channel_id}/access/{access_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}
