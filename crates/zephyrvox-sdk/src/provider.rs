//! Adapter from the HTTP client to the realtime snapshot-provider boundary.

use std::sync::Arc;

use zephyrvox_http::{ApiClient, HttpError};
use zephyrvox_realtime::{AccessToken, BoxFuture, RealtimeError, SnapshotProvider};
use zephyrvox_types::StateSnapshot;

/// Supplies authenticated snapshots and access tokens from one shared HTTP
/// client.
pub(crate) struct HttpSnapshotProvider {
    api: ApiClient,
}

impl HttpSnapshotProvider {
    /// Creates a provider that shares credential and refresh state with `api`.
    pub(crate) fn new(api: ApiClient) -> Self {
        Self { api }
    }
}

impl SnapshotProvider for HttpSnapshotProvider {
    fn snapshot(&self) -> BoxFuture<'static, Result<StateSnapshot, RealtimeError>> {
        let api = self.api.clone();
        Box::pin(async move { api.snapshot().await.map_err(map_http_error) })
    }

    fn access_token(&self) -> BoxFuture<'static, Result<AccessToken, RealtimeError>> {
        let api = self.api.clone();
        Box::pin(async move {
            let tokens = api.load_credentials().await.map_err(map_http_error)?;
            let tokens = tokens.ok_or(RealtimeError::AuthenticationRequired)?;
            AccessToken::new(tokens.access_token().as_str()).map_err(|_| {
                RealtimeError::Provider(
                    "credential store returned an invalid access token".to_owned(),
                )
            })
        })
    }
}

/// Maps HTTP lifecycle failures without exposing secret material to realtime.
fn map_http_error(error: HttpError) -> RealtimeError {
    match &error {
        HttpError::AuthenticationRequired => RealtimeError::AuthenticationRequired,
        HttpError::AuthExpired => RealtimeError::AuthenticationExpired,
        HttpError::Closed => RealtimeError::Closed,
        HttpError::TlsPinMismatch { expected, actual } => RealtimeError::TlsPinMismatch {
            expected: expected.clone(),
            actual: actual.clone(),
        },
        HttpError::TlsPeerCertificateInvalid | HttpError::TlsPeerCertificateUnavailable => {
            RealtimeError::TlsCertificateInvalid
        }
        HttpError::ProtocolVersionMismatch { expected, actual } => RealtimeError::Provider(
            format!("protocol version mismatch: expected {expected}, got {actual}"),
        ),
        _ => RealtimeError::Provider(error.to_string()),
    }
}

/// Returns an object-safe provider handle for a shared API client.
pub(crate) fn provider(api: ApiClient) -> Arc<dyn SnapshotProvider> {
    Arc::new(HttpSnapshotProvider::new(api))
}
