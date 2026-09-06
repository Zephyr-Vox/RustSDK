use serde::Deserialize;

use crate::{
    ActivateRequest, ApiClient, AuthStatus, HttpError, LoginRequest, LoginResult, MeResult,
    RegisterRequest, RegisterResult, RequestOptions, TokenPair, UserDetail,
    client::AuthRequirement, response::ApiResponse,
};
use zephyrvox_types::User;

/// Authentication and bootstrap endpoint accessor.
///
/// The accessor borrows an [`ApiClient`](crate::ApiClient); it is cheap to
/// create and does not own a separate connection or credential store.
pub struct AuthApi<'a> {
    client: &'a ApiClient,
}

impl<'a> AuthApi<'a> {
    pub(crate) const fn new(client: &'a ApiClient) -> Self {
        Self { client }
    }

    /// Fetches unauthenticated registration/bootstrap status.
    ///
    /// The result tells a host whether to show first-owner activation and which
    /// registration mode the server currently accepts.
    pub async fn status(&self) -> Result<AuthStatus, HttpError> {
        let request = self.client.empty_request(
            crate::HttpMethod::Get,
            "auth/status",
            RequestOptions::new(),
        )?;
        Ok(self
            .client
            .send_json(request, AuthRequirement::None)
            .await?
            .data)
    }

    /// Logs in and atomically persists the returned token pair.
    ///
    /// The pair becomes active only after the configured credential store
    /// accepts it.  The returned [`LoginResult`] keeps the same redacted token
    /// pair that was installed in the client.
    pub async fn login(&self, input: LoginRequest) -> Result<LoginResult, HttpError> {
        let request = self.client.json_request(
            crate::HttpMethod::Post,
            "auth/login",
            &input,
            RequestOptions::new(),
        )?;
        let response: ApiResponse<LoginWireResponse> = self
            .client
            .send_json(request, AuthRequirement::None)
            .await?;
        let tokens = TokenPair::from_expires_in(
            response.data.access_token,
            response.data.refresh_token,
            response.data.expires_in,
        )?;
        self.client.set_tokens(tokens.clone()).await?;
        Ok(LoginResult {
            tokens,
            user: response.data.user.into_user(),
        })
    }

    /// Registers an account without issuing tokens.
    ///
    /// When `input.invite` is `None`, the invite carried by the server card is
    /// copied into this one request.  The server still validates the invite;
    /// the card does not bypass registration policy.
    pub async fn register(&self, input: RegisterRequest) -> Result<RegisterResult, HttpError> {
        self.register_with_options(input, RequestOptions::mutation())
            .await
    }

    /// Registers an account with a caller-selected retry identity.
    ///
    /// A missing invite is filled from the server card just as it is for
    /// [`Self::register`].
    ///
    /// The same [`RequestOptions`] value should be reused when the caller
    /// retries after an unknown network outcome.  Reusing the key with a
    /// different request body is rejected by the server.  Use
    /// [`Self::register_response_with_options`] when the state headers from
    /// the successful mutation are needed.
    pub async fn register_with_options(
        &self,
        input: RegisterRequest,
        options: RequestOptions,
    ) -> Result<RegisterResult, HttpError> {
        Ok(self
            .register_response_with_options(input, options)
            .await?
            .data)
    }

    /// Registers an account and preserves the server's state headers.
    pub async fn register_response(
        &self,
        input: RegisterRequest,
    ) -> Result<ApiResponse<RegisterResult>, HttpError> {
        self.register_response_with_options(input, RequestOptions::mutation())
            .await
    }

    /// Registers an account with a caller-selected retry identity and
    /// preserves the server's state headers.
    ///
    /// The server may return a command ID, state cursor, stream epoch, GEID,
    /// or a sync-required marker because registration changes visible state.
    /// This method retains that metadata in [`ApiResponse::headers`].
    pub async fn register_response_with_options(
        &self,
        mut input: RegisterRequest,
        mut options: RequestOptions,
    ) -> Result<ApiResponse<RegisterResult>, HttpError> {
        if input.invite.is_none() {
            input.invite = self.client.server_card().invite().map(str::to_owned);
        }
        options.ensure_idempotency();
        let request =
            self.client
                .json_request(crate::HttpMethod::Post, "auth/register", &input, options)?;
        let response: ApiResponse<UserEnvelope> = self
            .client
            .send_json(request, AuthRequirement::None)
            .await?;
        Ok(ApiResponse {
            data: RegisterResult {
                user: response.data.user.into_user(),
            },
            status: response.status,
            headers: response.headers,
        })
    }

    /// Activates the first-owner account without issuing tokens.
    ///
    /// The caller must log in separately after a successful activation.  Use
    /// [`Self::activate_response_with_options`] when the state headers from
    /// activation are needed.
    pub async fn activate(&self, input: ActivateRequest) -> Result<User, HttpError> {
        self.activate_with_options(input, RequestOptions::mutation())
            .await
    }

    /// Activates the first-owner account with a caller-selected retry identity.
    ///
    /// The same [`RequestOptions`] value should be reused when the caller
    /// retries after an unknown network outcome.  Reusing the key with a
    /// different activation request is rejected by the server.  Use
    /// [`Self::activate_response_with_options`] when the state headers from
    /// the successful mutation are needed.
    pub async fn activate_with_options(
        &self,
        input: ActivateRequest,
        options: RequestOptions,
    ) -> Result<User, HttpError> {
        Ok(self
            .activate_response_with_options(input, options)
            .await?
            .data)
    }

    /// Activates the first-owner account and preserves the server's state
    /// headers.
    pub async fn activate_response(
        &self,
        input: ActivateRequest,
    ) -> Result<ApiResponse<User>, HttpError> {
        self.activate_response_with_options(input, RequestOptions::mutation())
            .await
    }

    /// Activates the first-owner account with a caller-selected retry identity
    /// and preserves the server's state headers.
    ///
    /// Activation may return a command ID and state checkpoint headers even
    /// though it does not issue authentication tokens.
    pub async fn activate_response_with_options(
        &self,
        input: ActivateRequest,
        mut options: RequestOptions,
    ) -> Result<ApiResponse<User>, HttpError> {
        options.ensure_idempotency();
        let request =
            self.client
                .json_request(crate::HttpMethod::Post, "admin/activate", &input, options)?;
        let response: ApiResponse<UserEnvelope> = self
            .client
            .send_json(request, AuthRequirement::None)
            .await?;
        Ok(ApiResponse {
            data: response.data.user.into_user(),
            status: response.status,
            headers: response.headers,
        })
    }

    /// Performs one refresh singleflight and returns the rotated pair.
    ///
    /// Concurrent callers wait for the same refresh request.  A rejected
    /// refresh clears local credentials and returns [`HttpError::AuthExpired`]
    /// to all waiters.  If clearing the injected store fails, that storage
    /// error is shared with the waiters instead.
    pub async fn refresh(&self) -> Result<TokenPair, HttpError> {
        self.client.refresh_access().await
    }

    /// Revokes the refresh token and clears local credentials after `204`.
    ///
    /// Logout is intentionally sent without an access-token header because the
    /// server authenticates this endpoint with the refresh token in the body.
    pub async fn logout(&self) -> Result<(), HttpError> {
        self.logout_with_options(RequestOptions::mutation()).await
    }

    /// Revokes the refresh token with a caller-selected retry identity.
    pub async fn logout_with_options(&self, mut options: RequestOptions) -> Result<(), HttpError> {
        options.ensure_idempotency();
        let Some(tokens) = self.client.load_credentials().await? else {
            return Ok(());
        };
        let input = LogoutRequest {
            refresh_token: tokens.refresh_token().as_str().to_owned(),
        };
        let request =
            self.client
                .json_request(crate::HttpMethod::Post, "auth/logout", &input, options)?;
        self.client
            .send_no_content(request, AuthRequirement::None)
            .await?;
        self.client.clear_tokens().await
    }

    /// Fetches the current profile and effective permissions.
    ///
    /// The returned permissions are the server's effective, deduplicated
    /// permission projection and are not cached by this accessor.
    pub async fn me(&self) -> Result<MeResult, HttpError> {
        let request =
            self.client
                .empty_request(crate::HttpMethod::Get, "auth/me", RequestOptions::new())?;
        let response: ApiResponse<MeWireResponse> = self
            .client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await?;
        let MeWireResponse {
            id,
            username,
            nickname,
            avatar,
            permissions,
        } = response.data;
        Ok(MeResult {
            user: User {
                id,
                username,
                nickname,
                avatar: avatar.filter(|avatar| !avatar.is_empty()),
            },
            permissions,
        })
    }

    /// Lists detailed users for callers with the server's user-read permission.
    pub async fn list_users(&self, limit: u32, offset: u32) -> Result<Vec<UserDetail>, HttpError> {
        let path = format!("users?limit={limit}&offset={offset}");
        let request =
            self.client
                .empty_request(crate::HttpMethod::Get, &path, RequestOptions::new())?;
        let response: ApiResponse<Vec<UserDetail>> = self
            .client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await?;
        Ok(response.data)
    }
}

#[derive(Deserialize)]
struct LoginWireResponse {
    access_token: String,
    refresh_token: String,
    expires_in: i64,
    user: WireUser,
}

#[derive(Deserialize)]
struct UserEnvelope {
    user: WireUser,
}

#[derive(serde::Serialize)]
struct LogoutRequest {
    refresh_token: String,
}

#[derive(Deserialize)]
struct MeWireResponse {
    id: zephyrvox_types::Snowflake,
    username: String,
    nickname: String,
    avatar: Option<String>,
    permissions: Vec<String>,
}

#[derive(Deserialize)]
struct WireUser {
    id: zephyrvox_types::Snowflake,
    username: String,
    nickname: String,
    avatar: Option<String>,
}

impl WireUser {
    fn into_user(self) -> User {
        User {
            id: self.id,
            username: self.username,
            nickname: self.nickname,
            avatar: self.avatar.filter(|avatar| !avatar.is_empty()),
        }
    }
}
