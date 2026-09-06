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
    /// A Snowflake was negative even though server identifiers are positive.
    #[error("snowflake must not be negative")]
    NegativeSnowflake,
    /// A decimal identifier could not be parsed.
    #[error("invalid decimal identifier: {0}")]
    InvalidDecimal(String),
    /// A fixed-width hexadecimal identifier had the wrong length.
    #[error("hex value must contain {expected} characters, got {actual}")]
    InvalidHexLength {
        /// The required number of hexadecimal characters.
        expected: usize,
        /// The number of characters supplied by the caller.
        actual: usize,
    },
    /// A fixed-width hexadecimal identifier contained a non-hex character.
    #[error("hex value contains an invalid character at index {index}")]
    InvalidHexCharacter {
        /// The zero-based byte index of the invalid character.
        index: usize,
    },
    /// A fixed-width identifier was all zero.
    #[error("identifier must not be all zero")]
    ZeroIdentifier,
    /// A cursor was empty.
    #[error("cursor must not be empty")]
    EmptyCursor,
    /// A cursor exceeded the v1 wire length limit.
    #[error("cursor must contain at most {max} characters, got {actual}")]
    CursorTooLong {
        /// The maximum number of encoded cursor characters.
        max: usize,
        /// The number of encoded cursor characters supplied by the caller.
        actual: usize,
    },
    /// A cursor contained a character outside unpadded base64url.
    #[error("cursor contains an invalid character at index {index}")]
    InvalidCursorCharacter {
        /// The zero-based byte index of the invalid character.
        index: usize,
    },
    /// An entity tag was empty.
    #[error("ETag must not be empty")]
    EmptyEtag,
}
