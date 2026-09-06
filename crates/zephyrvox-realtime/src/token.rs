use std::fmt;

/// A non-empty access token accepted by the WebSocket transport.
///
/// The value is deliberately separate from HTTP's credential types so the
/// realtime crate does not depend on the HTTP crate. Its debug representation
/// is redacted, and callers must not place the value in URLs or diagnostics.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AccessToken(String);

impl AccessToken {
    /// Creates an access token after rejecting an empty value.
    ///
    /// # Errors
    ///
    /// Returns [`crate::RealtimeError::Configuration`] when `value` is empty.
    pub fn new(value: impl Into<String>) -> Result<Self, crate::RealtimeError> {
        let value = value.into();
        if value.is_empty() {
            return Err(crate::RealtimeError::Configuration(
                "access token must not be empty".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the secret token for the one HTTP upgrade header.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted-access-token>")
    }
}
