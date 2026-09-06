use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, Serializer};

use crate::pagination::query_path;
use crate::{
    ApiClient, ApiResponse, HttpError, HttpMethod, RequestOptions, client::AuthRequirement,
};
use zephyrvox_types::Snowflake;

/// Serializes a Snowflake as the JSON number required by the RBAC wire API.
fn serialize_snowflake_number<S>(value: &Snowflake, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u64(value.get())
}

/// Serializes an optional RBAC scope identifier as a JSON number when present.
fn serialize_optional_snowflake_number<S>(
    value: &Option<Snowflake>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.get()),
        None => serializer.serialize_none(),
    }
}

/// Server, group, or channel scope used by RBAC endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RbacScope {
    /// Scope kind: `server`, `group`, or `channel`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Group or channel identifier; omitted for server scope.
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_snowflake_number"
    )]
    pub id: Option<Snowflake>,
}

/// Role record returned by the RBAC control plane.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RoleRecord {
    /// Immutable role key.
    pub key: String,
    /// Editable display name.
    pub display_name: String,
    /// Role ordering rank.
    pub rank: i64,
    /// Whether the role is built in.
    pub builtin: bool,
    /// Whether the role key or policy is immutable.
    pub immutable: bool,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
    /// Last update time in Unix milliseconds.
    pub updated_at: i64,
    /// Persisted row version.
    pub version: i64,
}

/// Request body for creating a role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateRoleRequest {
    /// Immutable role key.
    pub key: String,
    /// Display name.
    pub display_name: String,
    /// Role ordering rank.
    pub rank: i64,
}

/// Partial update body for a role.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Default)]
pub struct UpdateRoleRequest {
    /// Replacement display name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Replacement rank.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<i64>,
}

/// Role binding returned by the RBAC control plane.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RoleBinding {
    /// Binding identifier.
    pub id: Snowflake,
    /// Bound user identifier.
    pub user_id: Snowflake,
    /// Role key.
    pub role_key: String,
    /// Binding scope.
    pub scope: RbacScope,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
}

/// Request body for creating a role binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateBindingRequest {
    /// User identifier.
    #[serde(serialize_with = "serialize_snowflake_number")]
    pub user_id: Snowflake,
    /// Role key.
    pub role_key: String,
    /// Binding scope.
    pub scope: RbacScope,
}

/// Permission configuration returned by the RBAC control plane.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PermissionConfig {
    /// Requested scope.
    pub scope: RbacScope,
    /// Local override, or `None` when the scope inherits.
    pub local: Option<BTreeMap<String, Vec<String>>>,
    /// Local configuration version when an override exists.
    pub local_version: Option<i64>,
    /// Nearest source scope supplying inherited values.
    pub source: RbacScope,
    /// Source configuration version.
    pub source_version: i64,
    /// Effective permission configuration.
    pub effective: BTreeMap<String, Vec<String>>,
}

/// Request body for replacing a permission configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpdatePermissionConfigRequest {
    /// Scope being configured.
    pub scope: RbacScope,
    /// Local permission map.
    pub config: BTreeMap<String, Vec<String>>,
}

/// Result of an owner transfer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct OwnerTransferResponse {
    /// Previous owner identifier.
    pub previous_owner_id: Snowflake,
    /// New owner identifier.
    pub new_owner_id: Snowflake,
}

/// Typed accessor for the RBAC control plane.
pub struct RbacApi<'a> {
    client: &'a ApiClient,
}

impl ApiClient {
    /// Returns the RBAC resource accessor.
    pub fn rbac(&self) -> RbacApi<'_> {
        RbacApi { client: self }
    }
}

impl RbacApi<'_> {
    /// Lists all server roles.
    pub async fn list_roles(&self) -> Result<ApiResponse<Vec<RoleRecord>>, HttpError> {
        let request =
            self.client
                .empty_request(HttpMethod::Get, "rbac/roles", RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Creates a role.
    pub async fn create_role(
        &self,
        input: CreateRoleRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<RoleRecord>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .json_request(HttpMethod::Post, "rbac/roles", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Updates a role.
    pub async fn update_role(
        &self,
        key: &str,
        input: UpdateRoleRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<RoleRecord>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let path = role_path(key)?;
        let request = self
            .client
            .json_request(HttpMethod::Patch, &path, &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Deletes a role.
    pub async fn delete_role(
        &self,
        key: &str,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let path = role_path(key)?;
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Lists role bindings visible to the caller.
    pub async fn list_bindings(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Vec<RoleBinding>>, HttpError> {
        let path = query_path(
            "rbac/bindings",
            [("limit", limit.to_string()), ("offset", offset.to_string())],
        );
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Creates or reuses a role binding.
    pub async fn create_binding(
        &self,
        input: CreateBindingRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<RoleBinding>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let request =
            self.client
                .json_request(HttpMethod::Post, "rbac/bindings", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Deletes a role binding.
    pub async fn delete_binding(
        &self,
        binding_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let path = format!("rbac/bindings/{binding_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Transfers ownership to another user.
    pub async fn transfer_owner(
        &self,
        target_user_id: Snowflake,
        options: RequestOptions,
    ) -> Result<ApiResponse<OwnerTransferResponse>, HttpError> {
        let mut options = options;
        options.ensure_idempotency();
        let input = OwnerTransferRequest { target_user_id };
        let request =
            self.client
                .json_request(HttpMethod::Post, "owner/transfer", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Fetches effective permission configuration and its ETag.
    pub async fn get_config(
        &self,
        scope: RbacScope,
    ) -> Result<ApiResponse<PermissionConfig>, HttpError> {
        let path = scope_query("rbac/config", &scope);
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Replaces permission configuration after an ETag precondition check.
    pub async fn update_config(
        &self,
        input: UpdatePermissionConfigRequest,
        options: RequestOptions,
    ) -> Result<ApiResponse<PermissionConfig>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let request = self
            .client
            .json_request(HttpMethod::Put, "rbac/config", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Resets a scope to inherited permission configuration.
    pub async fn reset_config(
        &self,
        scope: RbacScope,
        options: RequestOptions,
    ) -> Result<ApiResponse<PermissionConfig>, HttpError> {
        options.require_if_match()?;
        let mut options = options;
        options.ensure_idempotency();
        let input = ResetPermissionConfigRequest { scope };
        let request =
            self.client
                .json_request(HttpMethod::Post, "rbac/config/reset", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct OwnerTransferRequest {
    #[serde(serialize_with = "serialize_snowflake_number")]
    target_user_id: Snowflake,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct ResetPermissionConfigRequest {
    scope: RbacScope,
}

fn scope_query(resource: &str, scope: &RbacScope) -> String {
    let mut parameters = vec![("scope_type", scope.kind.clone())];
    if let Some(id) = scope.id {
        parameters.push(("scope_id", id.to_string()));
    }
    query_path(resource, parameters)
}

/// Validates and constructs a role path segment using the server's key grammar.
fn role_path(key: &str) -> Result<String, HttpError> {
    let valid = (1..=64).contains(&key.len())
        && key.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && key.bytes().skip(1).all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_.-".contains(&byte)
        });
    if !valid {
        return Err(HttpError::Wire("invalid role key".to_owned()));
    }
    Ok(format!("rbac/roles/{key}"))
}
