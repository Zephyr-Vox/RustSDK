use std::{fmt, sync::Arc, time::Duration};

use tokio::sync::RwLock;

use crate::{BoxFuture, errors::CredentialStoreError};

/// A non-empty bearer access token with redacted [`Debug`](std::fmt::Debug)
/// output.
///
/// The value is intended for an `Authorization` header only.  It is never
/// included in the SDK's request/response debug representations.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AccessToken(String);

impl AccessToken {
    /// Creates a non-empty access token.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialStoreError::InvalidTokenPair`] for an empty value.
    pub fn new(value: impl Into<String>) -> Result<Self, CredentialStoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CredentialStoreError::InvalidTokenPair);
        }
        Ok(Self(value))
    }

    /// Returns the token for constructing an `Authorization` header.
    ///
    /// The returned string is secret material and must not be logged, persisted
    /// in an unprotected form, or placed in a URL.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted-access-token>")
    }
}

/// A non-empty refresh token with redacted [`Debug`](std::fmt::Debug) output.
///
/// Refresh tokens are accepted only by the refresh and logout endpoints.  They
/// must not be sent over WebSocket or UDP transports.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RefreshToken(String);

impl RefreshToken {
    /// Creates a non-empty refresh token.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialStoreError::InvalidTokenPair`] for an empty value.
    pub fn new(value: impl Into<String>) -> Result<Self, CredentialStoreError> {
        let value = value.into();
        if value.is_empty() {
            return Err(CredentialStoreError::InvalidTokenPair);
        }
        Ok(Self(value))
    }

    /// Returns the raw token for persistence or an explicit refresh/logout request.
    ///
    /// Callers must treat the returned value as a secret and must not include it
    /// in logs, diagnostics, URLs, or error messages.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RefreshToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted-refresh-token>")
    }
}

/// The rotated access/refresh pair held by one authenticated client.
///
/// `TokenPair` deliberately does not expose its fields publicly and redacts
/// both credentials from [`Debug`](std::fmt::Debug).  A successful refresh
/// replaces the pair as one unit; callers should not persist either token
/// independently.
#[derive(Clone, PartialEq, Eq)]
pub struct TokenPair {
    access_token: AccessToken,
    refresh_token: RefreshToken,
    expires_in: Duration,
}

impl TokenPair {
    /// Creates a token pair with its server-provided lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`CredentialStoreError::InvalidTokenPair`] when `expires_in` is
    /// zero.
    pub fn new(
        access_token: AccessToken,
        refresh_token: RefreshToken,
        expires_in: Duration,
    ) -> Result<Self, CredentialStoreError> {
        if expires_in.is_zero() {
            return Err(CredentialStoreError::InvalidTokenPair);
        }
        Ok(Self {
            access_token,
            refresh_token,
            expires_in,
        })
    }

    /// Creates a token pair from the server's integer lifetime.
    pub(crate) fn from_expires_in(
        access_token: impl Into<String>,
        refresh_token: impl Into<String>,
        expires_in: i64,
    ) -> Result<Self, CredentialStoreError> {
        if expires_in <= 0 {
            return Err(CredentialStoreError::InvalidTokenPair);
        }
        Self::new(
            AccessToken::new(access_token)?,
            RefreshToken::new(refresh_token)?,
            Duration::from_secs(expires_in as u64),
        )
    }

    /// Returns the current access token for an authenticated HTTP request.
    pub fn access_token(&self) -> &AccessToken {
        &self.access_token
    }

    /// Returns the refresh token for an explicit refresh or logout request.
    pub fn refresh_token(&self) -> &RefreshToken {
        &self.refresh_token
    }

    /// Returns the server-provided access-token lifetime.
    pub fn expires_in(&self) -> Duration {
        self.expires_in
    }
}

impl fmt::Debug for TokenPair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TokenPair")
            .field("access_token", &self.access_token)
            .field("refresh_token", &self.refresh_token)
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

/// A host-provided asynchronous credential store.
///
/// Implementations are shared by concurrent [`crate::ApiClient`] operations,
/// must be object-safe, and must not include plaintext credentials in errors or
/// debug output.  The SDK calls [`CredentialStore::save`] only after a server
/// login or refresh has returned a complete rotated pair.
pub trait CredentialStore: Send + Sync {
    /// Loads the current token pair, if one is persisted.
    ///
    /// Returning `Ok(None)` means that the client is anonymous.  A storage
    /// failure must be returned as `Err` rather than treated as an empty store.
    fn load(&self) -> BoxFuture<'_, Result<Option<TokenPair>, CredentialStoreError>>;
    /// Atomically persists a rotated token pair from the SDK's perspective.
    ///
    /// The SDK does not make the pair active when this future returns an error.
    fn save(&self, tokens: &TokenPair) -> BoxFuture<'_, Result<(), CredentialStoreError>>;
    /// Removes the current token pair.
    ///
    /// The SDK clears its in-memory pair only after this operation succeeds.
    fn clear(&self) -> BoxFuture<'_, Result<(), CredentialStoreError>>;
}

/// An in-memory credential store suitable as the default and for tests.
///
/// Clones share one asynchronous slot.  Dropping the last clone releases the
/// slot; this type never writes credentials to disk.
#[derive(Clone, Default)]
pub struct MemoryCredentialStore {
    tokens: Arc<RwLock<Option<TokenPair>>>,
}

impl MemoryCredentialStore {
    /// Creates an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl CredentialStore for MemoryCredentialStore {
    fn load(&self) -> BoxFuture<'_, Result<Option<TokenPair>, CredentialStoreError>> {
        Box::pin(async move { Ok(self.tokens.read().await.clone()) })
    }

    fn save(&self, tokens: &TokenPair) -> BoxFuture<'_, Result<(), CredentialStoreError>> {
        let tokens = tokens.clone();
        Box::pin(async move {
            *self.tokens.write().await = Some(tokens);
            Ok(())
        })
    }

    fn clear(&self) -> BoxFuture<'_, Result<(), CredentialStoreError>> {
        Box::pin(async move {
            *self.tokens.write().await = None;
            Ok(())
        })
    }
}
