use serde::{Deserialize, Serialize};

use crate::{
    ApiClient, ApiResponse, HttpError, HttpMethod, Page, RequestOptions,
    client::AuthRequirement,
    pagination::{page_path, query_path},
};
use zephyrvox_types::{Group, Snowflake};

/// Group/channel ACL principal accepted by the CommunityServer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessPrincipal {
    /// A user principal.
    User(Snowflake),
    /// A server role principal.
    Role(String),
}

/// One group or channel ACL entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessEntry {
    /// ACL entry identifier.
    pub id: Snowflake,
    /// Wire principal kind, currently `user` or `role`.
    pub principal_type: String,
    /// User principal, when `principal_type` is `user`.
    pub user_id: Option<Snowflake>,
    /// Role principal, when `principal_type` is `role`.
    pub role_key: Option<String>,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
}

/// Request body for creating a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateGroupRequest {
    /// Display name.
    pub name: String,
    /// UI ordering position.
    pub position: i64,
    /// Visibility policy, normally `public` or `private`.
    pub visibility: String,
}

/// Partial update body for a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct UpdateGroupRequest {
    /// Replacement display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Replacement UI ordering position.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<i64>,
    /// Replacement visibility policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
}

/// Typed accessor for `/groups` and group ACL endpoints.
pub struct GroupApi<'a> {
    client: &'a ApiClient,
}

impl ApiClient {
    /// Returns the group resource accessor.
    pub fn groups(&self) -> GroupApi<'_> {
        GroupApi { client: self }
    }
}

impl GroupApi<'_> {
    /// Lists visible groups using the server's bounded pagination contract.
    pub async fn list(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Page<Group>>, HttpError> {
        let path = page_path("groups", limit, offset);
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Fetches one visible group and its current response ETag.
    pub async fn get(&self, group_id: Snowflake) -> Result<ApiResponse<Group>, HttpError> {
        let path = format!("groups/{group_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Creates a group and returns the committed state metadata.
    pub async fn create(
        &self,
        input: CreateGroupRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<Group>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .json_request(HttpMethod::Post, "groups", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Updates a group after the caller supplies its current ETag in options.
    pub async fn update(
        &self,
        group_id: Snowflake,
        input: UpdateGroupRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<Group>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("groups/{group_id}");
        let request = self
            .client
            .json_request(HttpMethod::Patch, &path, &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Deletes a group after the caller supplies its current ETag in options.
    pub async fn delete(
        &self,
        group_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("groups/{group_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Lists ACL entries for a group and returns the group's current ETag.
    pub async fn list_access(
        &self,
        group_id: Snowflake,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Page<AccessEntry>>, HttpError> {
        let path = page_path(&format!("groups/{group_id}/access"), limit, offset);
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Adds or reuses a group ACL entry using the group's ETag precondition.
    pub async fn add_access(
        &self,
        group_id: Snowflake,
        principal: AccessPrincipal,
        options: RequestOptions,
    ) -> Result<ApiResponse<AccessEntry>, HttpError> {
        options.require_if_match()?;
        self.mutate_access(group_id, principal, options, false)
            .await
    }

    /// Removes a group ACL entry using the group's ETag precondition.
    pub async fn delete_access(
        &self,
        group_id: Snowflake,
        access_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("groups/{group_id}/access/{access_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    async fn mutate_access(
        &self,
        group_id: Snowflake,
        principal: AccessPrincipal,
        options: RequestOptions,
        grant_parent: bool,
    ) -> Result<ApiResponse<AccessEntry>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let body = AccessRequest::from_principal(principal, grant_parent);
        let path = format!("groups/{group_id}/access");
        let request = self
            .client
            .json_request(HttpMethod::Post, &path, &body, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}

#[derive(Serialize)]
pub(crate) struct AccessRequest {
    principal_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<Snowflake>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role_key: Option<String>,
    grant_parent: bool,
}

impl AccessRequest {
    fn from_principal(principal: AccessPrincipal, grant_parent: bool) -> Self {
        match principal {
            AccessPrincipal::User(user_id) => Self {
                principal_type: "user",
                user_id: Some(user_id),
                role_key: None,
                grant_parent,
            },
            AccessPrincipal::Role(role_key) => Self {
                principal_type: "role",
                user_id: None,
                role_key: Some(role_key),
                grant_parent,
            },
        }
    }
}

pub(crate) fn channel_access_request(
    principal: AccessPrincipal,
    grant_parent: bool,
) -> AccessRequest {
    AccessRequest::from_principal(principal, grant_parent)
}

pub(crate) fn access_path(resource: &str, limit: u32, offset: u32) -> String {
    query_path(
        resource,
        [("limit", limit.to_string()), ("offset", offset.to_string())],
    )
}
