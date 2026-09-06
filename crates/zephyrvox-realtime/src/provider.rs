use crate::{AccessToken, BoxFuture, RealtimeError};
use zephyrvox_types::StateSnapshot;

/// Supplies the authenticated snapshot and access lease to realtime.
///
/// The trait keeps the realtime crate independent from HTTP. In the finished
/// facade, `zephyrvox-sdk` adapts the HTTP `ApiClient` here; tests and
/// embedded hosts can provide an in-memory implementation. Implementations
/// should share the HTTP client's refresh singleflight and must never put a
/// refresh token in the returned [`AccessToken`].
pub trait SnapshotProvider: Send + Sync {
    /// Fetches a complete replacement snapshot.
    ///
    /// The future owns its result and may perform token refresh before the
    /// snapshot request. A provider error is returned to the connection task,
    /// which may retry it according to the reconnect policy.
    fn snapshot(&self) -> BoxFuture<'static, Result<StateSnapshot, RealtimeError>>;

    /// Returns the current access token for a WebSocket upgrade.
    ///
    /// Implementations must refresh or otherwise validate credentials before
    /// returning an unusable token. The token is sent only as an upgrade
    /// header by the realtime transport.
    fn access_token(&self) -> BoxFuture<'static, Result<AccessToken, RealtimeError>>;
}
