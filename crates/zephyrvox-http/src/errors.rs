use thiserror::Error;

use zephyrvox_wire::{ApiError, EnvelopeError};

/// Errors raised by the credential store.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CredentialStoreError {
    /// The store could not complete an operation.
    #[error("credential store operation failed: {0}")]
    Storage(String),
    /// A token pair contained an empty or invalid token.
    #[error("credential pair is invalid")]
    InvalidTokenPair,
}

/// Errors raised by the HTTP control-plane client.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HttpError {
    /// The underlying HTTP transport failed.
    #[error("HTTP transport failed: {0}")]
    Transport(String),
    /// A request URL could not be built.
    #[error("invalid HTTP URL: {0}")]
    InvalidUrl(String),
    /// A request or response header violated the wire grammar.
    #[error("invalid HTTP header: {0}")]
    InvalidHeader(String),
    /// A response body was not valid JSON.
    #[error("invalid JSON response: {0}")]
    Json(String),
    /// A response violated an endpoint-specific wire contract.
    #[error("wire contract violation: {0}")]
    Wire(String),
    /// The response envelope violated the shared wire contract.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// The server returned a structured API error.
    #[error(transparent)]
    Api(#[from] ApiError),
    /// The server rejected an optimistic-concurrency precondition.
    ///
    /// The wrapped [`ApiError`] retains the endpoint-local code, field errors,
    /// HTTP status, and retryability metadata while giving callers a stable
    /// type for stale `If-Match` or parent-ETag handling.
    #[error(transparent)]
    PreconditionFailed(ApiError),
    /// The server returned an unstructured HTTP status.
    #[error("HTTP status {status} from {endpoint}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Request path.
        endpoint: String,
    },
    /// A protected operation has no usable access token.
    #[error("authentication is required")]
    AuthenticationRequired,
    /// A refresh token was rejected or could not refresh the session.
    #[error("authentication expired")]
    AuthExpired,
    /// The server advertised a protocol version this SDK does not support.
    #[error("protocol version mismatch: expected {expected}, got {actual}")]
    ProtocolVersionMismatch {
        /// Protocol version required by this SDK.
        expected: u16,
        /// Protocol version advertised by the server.
        actual: u16,
    },
    /// The peer certificate did not match the server-card SPKI pin.
    #[error("TLS SPKI fingerprint mismatch: expected {expected}, got {actual}")]
    TlsPinMismatch {
        /// Card fingerprint.
        expected: String,
        /// Peer certificate fingerprint.
        actual: String,
    },
    /// The TLS transport did not expose a peer certificate for pin checking.
    #[error("TLS peer certificate was unavailable for fingerprint pinning")]
    TlsPeerCertificateUnavailable,
    /// The peer certificate could not be parsed far enough to find its SPKI.
    #[error("TLS peer certificate was invalid")]
    TlsPeerCertificateInvalid,
    /// A 204 response was returned where a JSON envelope was required.
    #[error("endpoint {endpoint} returned 204 instead of a JSON envelope")]
    UnexpectedNoContent {
        /// Request path.
        endpoint: String,
    },
    /// A 204 response contained a body.
    #[error("endpoint {endpoint} returned a body with 204 No Content")]
    UnexpectedContent {
        /// Request path.
        endpoint: String,
    },
    /// A non-204 response was returned where no content was required.
    #[error("endpoint {endpoint} did not return 204 No Content")]
    ExpectedNoContent {
        /// Request path.
        endpoint: String,
    },
    /// A request was attempted after the client was closed.
    #[error("HTTP client is closed")]
    Closed,
    /// A voice control request was attempted without an active WebSocket identity.
    #[error("voice control request requires an active control connection")]
    MissingControlConnection,
    /// A mutation that requires optimistic concurrency was missing a precondition.
    #[error("missing required HTTP precondition header: {header}")]
    MissingPrecondition {
        /// Header that the endpoint requires.
        header: &'static str,
    },
    /// A credential store operation failed.
    #[error(transparent)]
    Credential(#[from] CredentialStoreError),
}
