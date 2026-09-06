use thiserror::Error;

/// Errors raised while parsing or constructing protocol identifiers.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IdError {
    /// A Snowflake was zero even though all server identities are positive.
    #[error("snowflake must be greater than zero")]
    ZeroSnowflake,
    /// A Snowflake exceeded the signed 63-bit range used by the server.
    #[error("snowflake {0} exceeds the signed 63-bit range")]
    SnowflakeOutOfRange(u64),
    /// A decimal identifier could not be parsed.
    #[error("invalid decimal identifier: {0}")]
    InvalidDecimal(String),
    /// A fixed-width hexadecimal identifier had the wrong length.
    #[error("hex value must contain {expected} characters, got {actual}")]
    InvalidHexLength { expected: usize, actual: usize },
    /// A fixed-width hexadecimal identifier contained a non-hex character.
    #[error("hex value contains an invalid character at index {index}")]
    InvalidHexCharacter { index: usize },
    /// A fixed-width identifier was all zero.
    #[error("identifier must not be all zero")]
    ZeroIdentifier,
    /// A cursor was empty.
    #[error("cursor must not be empty")]
    EmptyCursor,
    /// An entity tag was empty.
    #[error("ETag must not be empty")]
    EmptyEtag,
}
