use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::IdError;

const HEX: &[u8; 16] = b"0123456789abcdef";

fn decode_hex_16(value: &str) -> Result<[u8; 16], IdError> {
    if value.len() != 32 {
        return Err(IdError::InvalidHexLength {
            expected: 32,
            actual: value.len(),
        });
    }

    let bytes = value.as_bytes();
    let mut output = [0_u8; 16];
    for (index, pair) in bytes.chunks_exact(2).enumerate() {
        let high =
            lower_hex_value(pair[0]).ok_or(IdError::InvalidHexCharacter { index: index * 2 })?;
        let low = lower_hex_value(pair[1]).ok_or(IdError::InvalidHexCharacter {
            index: index * 2 + 1,
        })?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

fn lower_hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn encode_hex_16(value: &[u8; 16]) -> String {
    let mut output = String::with_capacity(32);
    for byte in value {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

macro_rules! fixed_id {
    ($name:ident) => {
        #[doc = "A non-zero 128-bit protocol identifier encoded as lowercase hex."]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; 16]);

        impl $name {
            /// Creates an identifier from its raw 128-bit value.
            pub fn from_bytes(value: [u8; 16]) -> Result<Self, IdError> {
                if value == [0_u8; 16] {
                    return Err(IdError::ZeroIdentifier);
                }
                Ok(Self(value))
            }

            /// Returns the raw identifier bytes.
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            /// Returns the canonical lowercase hexadecimal representation.
            pub fn to_hex(self) -> String {
                encode_hex_16(&self.0)
            }

            /// Parses exactly 32 lowercase hexadecimal characters.
            pub fn parse_hex(value: &str) -> Result<Self, IdError> {
                Self::from_bytes(decode_hex_16(value)?)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.to_hex())
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse_hex(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.to_hex())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::parse_hex(&value).map_err(de::Error::custom)
            }
        }
    };
}

fixed_id!(SessionId);
fixed_id!(ControlConnectionId);
fixed_id!(StreamEpoch);

/// The voice session identifier uses the same 128-bit wire representation as
/// the generic session identifier.
pub type VoiceSessionId = SessionId;
