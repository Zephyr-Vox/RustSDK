use bytes::Bytes;

use crate::{PacketCodec, PacketError, crypto};
use zephyrvox_types::{SessionId, Snowflake, StreamTypeId, VoiceSessionId};
use zephyrvox_wire::{
    CHANNEL_HEADER_SIZE, MAGIC, MAX_PACKET_SIZE, OUTER_HEADER_SIZE, PROTOCOL_VERSION, TAG_SIZE,
    is_business_stream_type,
};

/// Why a server-to-client control packet stopped the media session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevocationReason {
    /// A newer voice session replaced this one.
    Replaced,
    /// The server or control plane explicitly revoked the session.
    Revoked,
    /// A future server reason code was received and preserved.
    Unknown(u8),
}

impl RevocationReason {
    /// Converts the wire reason byte into a stable Rust value.
    pub const fn from_wire(value: u8) -> Self {
        match value {
            0x01 => Self::Replaced,
            0x02 => Self::Revoked,
            other => Self::Unknown(other),
        }
    }

    /// Returns the protocol reason byte for this value.
    pub const fn as_wire(self) -> u8 {
        match self {
            Self::Replaced => 0x01,
            Self::Revoked => 0x02,
            Self::Unknown(value) => value,
        }
    }
}

/// A non-media packet carried by the reserved stream type zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketControl {
    /// An empty heartbeat or NAT keepalive.
    Heartbeat,
    /// A one-byte best-effort server revocation notification.
    Revocation(RevocationReason),
}

/// A validated server-to-client packet before it is projected to media.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPacket {
    /// Session identifier carried by the datagram.
    pub session_id: VoiceSessionId,
    /// Server-to-client transport sequence.
    pub transport_seq: u64,
    /// Server-registered stream type.
    pub stream_type: StreamTypeId,
    /// Source channel sequence preserved by the relay.
    pub channel_seq: u16,
    /// Reserved channel flags, always zero in v1.
    pub flags: u8,
    /// Positive source user for media, absent for control packets.
    pub speaker_id: Option<Snowflake>,
    /// Encoded media payload, or the raw control payload.
    pub payload: Bytes,
    /// Heartbeat/revocation classification for stream type zero.
    pub control: Option<PacketControl>,
}

impl PacketCodec {
    /// Decodes and validates one server-to-client datagram.
    ///
    /// Replay-window admission is deliberately separate from parsing so the
    /// session task can authenticate first and then atomically classify the
    /// transport sequence.
    pub fn decode_server(&self, datagram: &[u8]) -> Result<DecodedPacket, PacketError> {
        self.decode_direction(datagram, false)
    }

    /// Decodes and validates one client-to-server datagram.
    ///
    /// This direction-aware helper is useful to transport test harnesses and
    /// relay adapters. A production [`VoiceSession`](crate::VoiceSession)
    /// normally only needs [`Self::decode_server`].
    pub fn decode_client(&self, datagram: &[u8]) -> Result<DecodedPacket, PacketError> {
        self.decode_direction(datagram, true)
    }

    /// Parses one packet and applies the direction-specific speaker/key rules.
    fn decode_direction(
        &self,
        datagram: &[u8],
        client_to_server: bool,
    ) -> Result<DecodedPacket, PacketError> {
        if datagram.len() < OUTER_HEADER_SIZE + CHANNEL_HEADER_SIZE {
            return Err(PacketError::Truncated);
        }
        if datagram.len() > MAX_PACKET_SIZE {
            return Err(PacketError::Oversized);
        }
        if datagram[..4] != MAGIC {
            return Err(PacketError::InvalidMagic);
        }
        if datagram[4] != PROTOCOL_VERSION {
            return Err(PacketError::UnsupportedVersion(datagram[4]));
        }
        if datagram[5] != 0 {
            return Err(PacketError::InvalidFlags);
        }
        let session_id = SessionId::from_bytes(datagram[6..22].try_into().expect("16 bytes"))
            .map_err(|_| PacketError::WrongSession)?;
        if session_id != self.session_id {
            return Err(PacketError::WrongSession);
        }
        let transport_seq = u64::from_be_bytes(
            datagram[22..30]
                .try_into()
                .expect("outer header contains eight sequence bytes"),
        );
        if transport_seq == 0 {
            return Err(PacketError::SequenceZero);
        }

        let body = if self.encrypted {
            if datagram.len() < OUTER_HEADER_SIZE + CHANNEL_HEADER_SIZE + TAG_SIZE {
                return Err(PacketError::Truncated);
            }
            let keys = self
                .keys
                .as_ref()
                .ok_or(PacketError::AuthenticationFailed)?;
            crypto::open(
                if client_to_server {
                    &keys.c2s
                } else {
                    &keys.s2c
                },
                transport_seq,
                &datagram[..OUTER_HEADER_SIZE],
                &datagram[OUTER_HEADER_SIZE..],
            )?
        } else {
            datagram[OUTER_HEADER_SIZE..].to_vec()
        };
        if body.len() < CHANNEL_HEADER_SIZE {
            return Err(PacketError::Truncated);
        }
        let stream_type = StreamTypeId::new(body[0]);
        let channel_seq = u16::from_be_bytes([body[1], body[2]]);
        let flags = body[3];
        if flags != 0 {
            return Err(PacketError::InvalidFlags);
        }
        let speaker_raw = u64::from_be_bytes(
            body[4..12]
                .try_into()
                .expect("channel header contains eight speaker bytes"),
        );
        let payload = &body[CHANNEL_HEADER_SIZE..];
        if stream_type.is_heartbeat() {
            if channel_seq != 0
                || speaker_raw != 0
                || payload.len() > 1
                || (client_to_server && !payload.is_empty())
            {
                return Err(PacketError::InvalidControlPacket);
            }
            let control = if payload.is_empty() {
                PacketControl::Heartbeat
            } else {
                PacketControl::Revocation(RevocationReason::from_wire(payload[0]))
            };
            return Ok(DecodedPacket {
                session_id,
                transport_seq,
                stream_type,
                channel_seq,
                flags,
                speaker_id: None,
                payload: Bytes::copy_from_slice(payload),
                control: Some(control),
            });
        }
        if !is_business_stream_type(stream_type.get())
            || !self.allowed_stream_types[stream_type.get() as usize]
        {
            return Err(PacketError::UnknownStreamType(stream_type.get()));
        }
        let speaker_id = if client_to_server {
            if speaker_raw != 0 {
                return Err(PacketError::InvalidSpeaker);
            }
            None
        } else {
            Some(Snowflake::new(speaker_raw).map_err(|_| PacketError::InvalidSpeaker)?)
        };
        if payload.len() > self.max_payload {
            return Err(PacketError::PayloadTooLarge {
                actual: payload.len(),
                maximum: self.max_payload,
            });
        }
        Ok(DecodedPacket {
            session_id,
            transport_seq,
            stream_type,
            channel_seq,
            flags,
            speaker_id,
            payload: Bytes::copy_from_slice(payload),
            control: None,
        })
    }
}
