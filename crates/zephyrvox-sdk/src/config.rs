//! Configuration for one high-level ZephyrVox client.

use std::{fmt, sync::Arc, time::Duration};

use zephyrvox_http::{CredentialStore, MemoryCredentialStore};
use zephyrvox_realtime::ReconnectPolicy;
use zephyrvox_wire::ServerCard;

use crate::SdkError;

/// Runtime policy for one [`crate::Client`].
///
/// The server card fixes the transport authority and TLS policy for the
/// client's entire lifetime.  The credential store is shared with the HTTP
/// adapter, so refresh rotation remains singleflight across all facade
/// operations.  `auto_rejoin_voice` is an explicit opt-in for ordinary
/// reconnects; authority-loss and explicit leave events always clear the
/// remembered intent.
pub struct ClientConfig {
    /// Parsed server card that fixes scheme, authority, protocol version, and
    /// optional TLS SPKI pin.
    pub server_card: ServerCard,
    /// Whole-request timeout used by the HTTP transport.
    pub request_timeout: Duration,
    /// Automatic reconnect policy for the WebSocket control connection.
    pub reconnect: ReconnectPolicy,
    /// Shared asynchronous credential storage.
    pub credential_store: Arc<dyn CredentialStore>,
    /// Whether the facade may retry the last voice join after an ordinary
    /// control-connection reconnect.
    ///
    /// The default is `false`. A server `VoiceLost` event, authentication
    /// revocation, and explicit leave always clear the intent, preventing a
    /// kick, ban, or revoke from being mistaken for a network interruption.
    pub auto_rejoin_voice: bool,
}

impl ClientConfig {
    /// Creates a configuration with the safe v1 defaults.
    pub fn new(server_card: ServerCard) -> Self {
        Self {
            server_card,
            request_timeout: Duration::from_secs(15),
            reconnect: ReconnectPolicy::default(),
            credential_store: Arc::new(MemoryCredentialStore::new()),
            auto_rejoin_voice: false,
        }
    }

    /// Replaces the HTTP request timeout.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::Configuration`] when `timeout` is zero.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Result<Self, SdkError> {
        if timeout.is_zero() {
            return Err(SdkError::configuration("request timeout must be non-zero"));
        }
        self.request_timeout = timeout;
        Ok(self)
    }

    /// Replaces the WebSocket reconnect policy.
    pub fn with_reconnect_policy(mut self, reconnect: ReconnectPolicy) -> Self {
        self.reconnect = reconnect;
        self
    }

    /// Replaces the credential store used by HTTP and realtime operations.
    pub fn with_credential_store(mut self, credential_store: Arc<dyn CredentialStore>) -> Self {
        self.credential_store = credential_store;
        self
    }

    /// Sets the explicit automatic voice-rejoin policy bit.
    pub const fn with_auto_rejoin_voice(mut self, enabled: bool) -> Self {
        self.auto_rejoin_voice = enabled;
        self
    }

    /// Validates values supplied through public fields or builders.
    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        if self.request_timeout.is_zero() {
            return Err(SdkError::configuration("request timeout must be non-zero"));
        }
        Ok(())
    }
}

impl Clone for ClientConfig {
    fn clone(&self) -> Self {
        Self {
            server_card: self.server_card.clone(),
            request_timeout: self.request_timeout,
            reconnect: self.reconnect,
            credential_store: Arc::clone(&self.credential_store),
            auto_rejoin_voice: self.auto_rejoin_voice,
        }
    }
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientConfig")
            .field("server_card", &self.server_card)
            .field("request_timeout", &self.request_timeout)
            .field("reconnect", &self.reconnect)
            .field("credential_store", &"<custom-store>")
            .field("auto_rejoin_voice", &self.auto_rejoin_voice)
            .finish()
    }
}
