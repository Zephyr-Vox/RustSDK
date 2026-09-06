use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::IdError;

/// An opaque entity tag used for HTTP optimistic concurrency.
///
/// The SDK preserves the server's exact tag text and never interprets weak
/// versus strong validators at the domain-model layer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Etag(String);

impl Etag {
    /// Creates a non-empty entity tag.
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdError::EmptyEtag);
        }
        Ok(Self(value))
    }

    /// Returns the exact entity-tag header value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Etag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Serialize for Etag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Etag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}
