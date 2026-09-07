use std::fmt;

use crate::decode::RevocationReason;
use crate::{PacketError, SessionKeyCache, VoiceError, crypto};
use zephyrvox_types::{Snowflake, StreamTypeId, VoiceSessionId};
use zephyrvox_wire::{
    CHANNEL_HEADER_SIZE, HEARTBEAT_STREAM_TYPE, MAGIC, MAX_PACKET_SIZE, OUTER_HEADER_SIZE,
    PROTOCOL_VERSION, is_business_stream_type,
};

/// Stateless wire codec bound to one negotiated voice session.
pub struct PacketCodec {
    pub(crate) session_id: VoiceSessionId,
    pub(crate) encrypted: bool,
    pub(crate) max_payload: usize,
    pub(crate) keys: Option<crypto::DirectionKeys>,
    pub(crate) allowed_stream_types: [bool; 256],
}

impl PacketCodec {
    /// Builds a codec from validated session negotiation and an optional key
    /// cache for reused encrypted sessions.
    pub fn from_config(
        config: &crate::VoiceSessionConfig,
        cache: Option<&SessionKeyCache>,
    ) -> Result<Self, VoiceError> {
        config.cache_inline_key(cache);
        let key = config.resolve_key(cache)?;
        let mut allowed = [true; 256];
        allowed[HEARTBEAT_STREAM_TYPE as usize] = false;
        if let Some(stream_types) = config.allowed_stream_types() {
            allowed.fill(false);
            for stream_type in stream_types {
                allowed[stream_type.get() as usize] = true;
            }
        }
        Ok(Self {
            session_id: config.session_id(),
            encrypted: config.encrypted(),
            max_payload: config.max_payload(),
            keys: key
                .as_ref()
                .map(|key| crypto::derive_direction_keys(config.session_id(), key)),
            allowed_stream_types: allowed,
        })
    }

    /// Returns the session identifier bound into the packet AAD.
    pub const fn session_id(&self) -> VoiceSessionId {
        self.session_id
    }

    /// Reports whether this codec authenticates packets with AES-GCM.
    pub const fn encrypted(&self) -> bool {
        self.encrypted
    }

    /// Returns the negotiated maximum encoded media payload length.
    pub const fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// Encodes one client-to-server media datagram.
    pub fn encode_media(
        &self,
        transport_seq: u64,
        channel_seq: u16,
        stream_type: StreamTypeId,
        payload: &[u8],
    ) -> Result<Vec<u8>, PacketError> {
        self.validate_media(stream_type, payload.len())?;
        self.encode_body(transport_seq, stream_type, channel_seq, 0, payload, true)
    }

    /// Encodes one empty client-to-server heartbeat datagram.
    pub fn encode_heartbeat(&self, transport_seq: u64) -> Result<Vec<u8>, PacketError> {
        self.encode_body(
            transport_seq,
            StreamTypeId::new(HEARTBEAT_STREAM_TYPE),
            0,
            0,
            &[],
            true,
        )
    }

    /// Encodes a server-to-client media datagram for relay and interoperability
    /// helpers.
    pub fn encode_server_media(
        &self,
        transport_seq: u64,
        channel_seq: u16,
        stream_type: StreamTypeId,
        speaker_id: Snowflake,
        payload: &[u8],
    ) -> Result<Vec<u8>, PacketError> {
        self.validate_media(stream_type, payload.len())?;
        self.encode_body(
            transport_seq,
            stream_type,
            channel_seq,
            speaker_id.get(),
            payload,
            false,
        )
    }

    /// Encodes a one-byte server-to-client revocation notification.
    pub fn encode_revocation(
        &self,
        transport_seq: u64,
        reason: RevocationReason,
    ) -> Result<Vec<u8>, PacketError> {
        self.encode_body(
            transport_seq,
            StreamTypeId::new(HEARTBEAT_STREAM_TYPE),
            0,
            0,
            &[reason.as_wire()],
            false,
        )
    }

    /// Validates a media stream and payload against the negotiated contract.
    pub(crate) fn validate_media(
        &self,
        stream_type: StreamTypeId,
        payload_length: usize,
    ) -> Result<(), PacketError> {
        if stream_type.is_heartbeat() {
            return Err(PacketError::HeartbeatStream);
        }
        if !is_business_stream_type(stream_type.get())
            || !self.allowed_stream_types[stream_type.get() as usize]
        {
            return Err(PacketError::UnknownStreamType(stream_type.get()));
        }
        if payload_length > self.max_payload {
            return Err(PacketError::PayloadTooLarge {
                actual: payload_length,
                maximum: self.max_payload,
            });
        }
        Ok(())
    }

    /// Builds one datagram body and applies direction-specific encryption.
    fn encode_body(
        &self,
        transport_seq: u64,
        stream_type: StreamTypeId,
        channel_seq: u16,
        speaker_id: u64,
        payload: &[u8],
        client_to_server: bool,
    ) -> Result<Vec<u8>, PacketError> {
        if transport_seq == 0 {
            return Err(PacketError::SequenceZero);
        }
        let mut header = vec![0_u8; OUTER_HEADER_SIZE];
        header[..4].copy_from_slice(&MAGIC);
        header[4] = PROTOCOL_VERSION;
        header[6..22].copy_from_slice(self.session_id.as_bytes());
        header[22..30].copy_from_slice(&transport_seq.to_be_bytes());
        let mut body = vec![0_u8; CHANNEL_HEADER_SIZE + payload.len()];
        body[0] = stream_type.get();
        body[1..3].copy_from_slice(&channel_seq.to_be_bytes());
        body[4..12].copy_from_slice(&speaker_id.to_be_bytes());
        body[CHANNEL_HEADER_SIZE..].copy_from_slice(payload);
        let body = if self.encrypted {
            let keys = self
                .keys
                .as_ref()
                .ok_or(PacketError::AuthenticationFailed)?;
            crypto::seal(
                if client_to_server {
                    &keys.c2s
                } else {
                    &keys.s2c
                },
                transport_seq,
                &header,
                &body,
            )?
        } else {
            body
        };
        let total = OUTER_HEADER_SIZE + body.len();
        if total > MAX_PACKET_SIZE {
            return Err(PacketError::Oversized);
        }
        header.extend_from_slice(&body);
        Ok(header)
    }
}

impl fmt::Debug for PacketCodec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PacketCodec")
            .field("session_id", &self.session_id)
            .field("encrypted", &self.encrypted)
            .field("max_payload", &self.max_payload)
            .finish_non_exhaustive()
    }
}
