use std::{fmt, str::FromStr};

use thiserror::Error;

/// Bearer access-token header.
pub const AUTHORIZATION: &str = "Authorization";
/// Mutation idempotency key header.
pub const IDEMPOTENCY_KEY: &str = "Idempotency-Key";
/// Resource ETag precondition header.
pub const IF_MATCH: &str = "If-Match";
/// Resource ETag response header.
pub const ETAG: &str = "ETag";
/// Parent resource ETag precondition header.
pub const PARENT_IF_MATCH: &str = "X-Zephyr-Parent-If-Match";
/// Parent resource ETag response header.
pub const PARENT_ETAG: &str = "X-Zephyr-Parent-ETag";
/// Active WebSocket control connection identifier.
pub const CONTROL_CONNECTION: &str = "X-Zephyr-Control-Connection";
/// Server command identifier returned by a committed mutation.
pub const COMMAND_ID: &str = "X-Zephyr-Command-ID";
/// Authenticated state cursor returned by a committed mutation.
pub const STATE_CURSOR: &str = "X-Zephyr-State-Cursor";
/// State stream epoch returned by a committed mutation.
pub const STREAM_EPOCH: &str = "X-Zephyr-Stream-Epoch";
/// Highest committed state GEID returned by a mutation.
pub const GEID: &str = "X-Zephyr-Geid";
/// Indicates that the client must obtain a replacement snapshot.
pub const SYNC_REQUIRED: &str = "X-Zephyr-Sync-Required";

/// The shortest idempotency key accepted by the HTTP mutation contract.
pub const MIN_IDEMPOTENCY_KEY_LENGTH: usize = 16;

/// The longest idempotency key accepted by the HTTP mutation contract.
pub const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 64;

/// A validated HTTP idempotency key.
///
/// The key is deliberately kept as caller-provided text: the server uses it
/// as the stable identity for retrying one logical mutation, while the SDK
/// only enforces the wire grammar.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Creates an idempotency key with the server's length and character rules.
    pub fn new(value: impl Into<String>) -> Result<Self, HeaderError> {
        let value = value.into();
        if !(MIN_IDEMPOTENCY_KEY_LENGTH..=MAX_IDEMPOTENCY_KEY_LENGTH).contains(&value.len()) {
            return Err(HeaderError::InvalidIdempotencyKeyLength {
                min: MIN_IDEMPOTENCY_KEY_LENGTH,
                max: MAX_IDEMPOTENCY_KEY_LENGTH,
                actual: value.len(),
            });
        }
        for (index, character) in value.bytes().enumerate() {
            if !matches!(character, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-') {
                return Err(HeaderError::InvalidIdempotencyKeyCharacter { index });
            }
        }
        Ok(Self(value))
    }

    /// Returns the exact header value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for IdempotencyKey {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for IdempotencyKey {
    type Err = HeaderError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Errors raised while validating HTTP header values with a fixed wire grammar.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum HeaderError {
    /// An idempotency key was outside the inclusive length range.
    #[error("idempotency key must contain {min}..={max} characters, got {actual}")]
    InvalidIdempotencyKeyLength {
        /// The minimum accepted character count.
        min: usize,
        /// The maximum accepted character count.
        max: usize,
        /// The supplied UTF-8 byte count.
        actual: usize,
    },
    /// An idempotency key contained a character outside `[A-Za-z0-9_-]`.
    #[error("idempotency key contains an invalid character at index {index}")]
    InvalidIdempotencyKeyCharacter {
        /// The zero-based byte index of the invalid character.
        index: usize,
    },
}
