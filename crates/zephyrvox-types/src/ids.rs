use std::{fmt, str::FromStr};

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, Visitor},
};

use crate::{IdError, fixed_ids::StreamEpoch};

const MAX_SNOWFLAKE: u64 = i64::MAX as u64;
/// A positive 63-bit Snowflake identifier.
///
/// Snowflakes serialize as decimal strings so JavaScript and other
/// number-limited consumers cannot lose precision.  Deserialization accepts
/// both the canonical string representation and a JSON integer for
/// interoperability with older server responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Snowflake(u64);

impl Snowflake {
    /// Creates a Snowflake after enforcing the server's positive 63-bit range.
    pub const fn new(value: u64) -> Result<Self, IdError> {
        if value == 0 {
            return Err(IdError::ZeroSnowflake);
        }
        if value > MAX_SNOWFLAKE {
            return Err(IdError::SnowflakeOutOfRange(value));
        }
        Ok(Self(value))
    }

    /// Returns the numeric Snowflake value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Snowflake {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for Snowflake {
    type Err = IdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let number = value
            .parse::<u64>()
            .map_err(|_| IdError::InvalidDecimal(value.to_owned()))?;
        Self::new(number)
    }
}

impl TryFrom<u64> for Snowflake {
    type Error = IdError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for Snowflake {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct SnowflakeVisitor;

impl<'de> Visitor<'de> for SnowflakeVisitor {
    type Value = Snowflake;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a positive 63-bit Snowflake encoded as a string or integer")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&value)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Snowflake::new(value).map_err(E::custom)
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value <= 0 {
            return Err(E::custom(IdError::ZeroSnowflake));
        }
        Snowflake::new(value as u64).map_err(E::custom)
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Err(E::custom("floating-point Snowflakes are not accepted"))
    }
}

impl<'de> Deserialize<'de> for Snowflake {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(SnowflakeVisitor)
    }
}

/// A globally ordered event identifier.
///
/// GEIDs may be zero in an uninitialized checkpoint, and therefore do not use
/// the positive Snowflake validation.  They still serialize as decimal text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Geid(u64);

impl Geid {
    /// Creates a GEID from its wire value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric GEID value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Geid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for Geid {
    type Err = IdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<u64>()
            .map(Self)
            .map_err(|_| IdError::InvalidDecimal(value.to_owned()))
    }
}

impl From<u64> for Geid {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

impl Serialize for Geid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

struct GeidVisitor;

impl<'de> Visitor<'de> for GeidVisitor {
    type Value = Geid;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GEID encoded as a decimal string or integer")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        value.parse().map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_str(&value)
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Geid::new(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value < 0 {
            return Err(E::custom("negative GEIDs are not accepted"));
        }
        Ok(Geid::new(value as u64))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Err(E::custom("floating-point GEIDs are not accepted"))
    }
}

impl<'de> Deserialize<'de> for Geid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(GeidVisitor)
    }
}

/// An authenticated opaque state cursor.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor(String);

impl Cursor {
    /// Creates a non-empty opaque cursor without interpreting its contents.
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdError::EmptyCursor);
        }
        Ok(Self(value))
    }

    /// Returns the exact opaque cursor text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for Cursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A stream checkpoint binding an opaque cursor position to an epoch and GEID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The process stream epoch.
    pub stream_epoch: StreamEpoch,
    /// The highest applied event identifier.
    pub geid: Geid,
}

/// A numeric media stream type.  Zero is reserved for heartbeat packets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamTypeId(u8);

impl StreamTypeId {
    /// Creates a stream type from the one-byte wire value.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// Returns the one-byte wire value.
    pub const fn get(self) -> u8 {
        self.0
    }

    /// Reports whether this is the reserved heartbeat stream type.
    pub const fn is_heartbeat(self) -> bool {
        self.0 == 0
    }
}
