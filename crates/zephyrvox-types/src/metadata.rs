use serde::{Deserialize, Serialize};

use crate::StreamTypeId;

/// Server metadata used to negotiate protocol and voice capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    /// The single protocol version covering HTTP, WebSocket, and UDP voice.
    pub protocol_version: u16,
    /// The endpoint used for the UDP voice transport.
    pub voice_endpoint: VoiceEndpoint,
    /// Codecs accepted by the server.
    pub codecs: Vec<CodecCapability>,
    /// Application stream types available to the client.
    pub voice_stream_types: Vec<VoiceStreamType>,
    /// Optional feature flags advertised by the server.
    pub features: Vec<String>,
}

/// A host and port advertised for UDP voice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceEndpoint {
    /// DNS name or IP address.
    pub host: String,
    /// UDP port.
    pub port: u16,
}

/// One negotiated audio codec capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecCapability {
    /// Codec name, for example opus.
    pub name: String,
    /// Audio sample rate in Hz.
    pub clock_rate: u32,
    /// Number of interleaved audio channels.
    pub channels: u8,
    /// Supported packetization times in milliseconds.
    pub ptime: Vec<u16>,
}

impl CodecCapability {
    /// Returns the advertised audio sampling rate.
    pub const fn sample_rate(&self) -> u32 {
        self.clock_rate
    }
}

/// A server-defined voice stream type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoiceStreamType {
    /// One-byte UDP stream identifier.
    pub id: StreamTypeId,
    /// Stable human-readable stream name.
    pub name: String,
    /// Mute policy associated with this stream.
    pub mute_kind: String,
}
