use std::io;

use thiserror::Error;

use crate::RevocationReason;

/// Errors raised by the UDP voice transport.
#[derive(Debug, Error)]
pub enum VoiceError {
    /// The negotiated values are inconsistent with the v1 wire contract.
    #[error("invalid voice configuration: {0}")]
    Configuration(String),
    /// The advertised endpoint could not be resolved or used.
    #[error("voice endpoint failed: {0}")]
    Endpoint(String),
    /// The UDP socket could not be opened, connected, sent, or received.
    #[error("voice UDP transport failed: {0}")]
    Transport(#[source] io::Error),
    /// A packet failed local encoding or decoding validation.
    #[error(transparent)]
    Packet(#[from] PacketError),
    /// An encrypted session did not have a usable master key.
    #[error("encrypted voice session key is unavailable")]
    KeyUnavailable,
    /// A configured or negotiated protocol version is unsupported.
    #[error("voice protocol version mismatch: expected {expected}, got {actual}")]
    ProtocolVersionMismatch {
        /// The only protocol version implemented by this crate.
        expected: u16,
        /// The version supplied by the server or host.
        actual: u16,
    },
    /// The media queue has reached its bounded capacity.
    #[error("voice media queue is full")]
    QueueFull,
    /// A media receiver fell behind the bounded broadcast history.
    #[error("voice media receiver lagged by {missed} frames")]
    MediaLagged {
        /// Number of frames overwritten before the receiver resumed.
        missed: u64,
    },
    /// The session task has already stopped accepting commands.
    #[error("voice session is closed")]
    Closed,
    /// The session is not ready to send media.
    #[error("voice session is not active")]
    NotActive,
    /// The transport sequence reached its non-wrapping upper bound.
    #[error("voice transport sequence is exhausted")]
    SequenceExhausted,
    /// The server or control plane revoked this voice session.
    #[error("voice session was revoked: {0:?}")]
    Revoked(RevocationReason),
}

/// Errors raised while encoding or validating one UDP datagram.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PacketError {
    /// The datagram is shorter than its required outer or channel header.
    #[error("voice datagram is truncated")]
    Truncated,
    /// The datagram exceeds the protocol's hard 1200-byte limit.
    #[error("voice datagram exceeds 1200 bytes")]
    Oversized,
    /// The four-byte datagram magic is not `ZVX1`.
    #[error("voice datagram has invalid magic")]
    InvalidMagic,
    /// The datagram uses a protocol version other than v1.
    #[error("unsupported voice protocol version {0}")]
    UnsupportedVersion(u8),
    /// Reserved outer or channel flag bits were set.
    #[error("voice datagram contains unsupported flags")]
    InvalidFlags,
    /// The packet session identifier does not match this voice session.
    #[error("voice datagram belongs to another session")]
    WrongSession,
    /// Sequence zero is reserved and is never accepted.
    #[error("voice transport sequence must start at one")]
    SequenceZero,
    /// AES-GCM authentication failed.
    #[error("voice datagram authentication failed")]
    AuthenticationFailed,
    /// A heartbeat or revocation subheader had an invalid shape.
    #[error("voice heartbeat or revocation packet is malformed")]
    InvalidControlPacket,
    /// A media packet had no positive speaker Snowflake.
    #[error("voice media packet has an invalid speaker id")]
    InvalidSpeaker,
    /// A stream type is not a registered business stream for this session.
    #[error("voice stream type {0} is not registered")]
    UnknownStreamType(u8),
    /// A media payload exceeds the negotiated or protocol hard limit.
    #[error("voice media payload is {actual} bytes, maximum is {maximum}")]
    PayloadTooLarge {
        /// Actual encoded payload length.
        actual: usize,
        /// Maximum length accepted for this session.
        maximum: usize,
    },
    /// A caller attempted to use the reserved heartbeat stream for media.
    #[error("voice stream type zero is reserved for heartbeat and revocation")]
    HeartbeatStream,
}
