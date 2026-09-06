use bytes::Bytes;

use crate::{Snowflake, StreamTypeId};

/// An encoded media frame sent to a voice session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMedia {
    /// The server-registered stream type.
    pub stream_type: StreamTypeId,
    /// Codec payload.  The SDK does not decode, mix, or transcode it.
    pub payload: Bytes,
}

/// An encoded media frame received from another voice participant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMedia {
    /// The user who produced this frame.
    pub speaker_id: Snowflake,
    /// The server-registered stream type.
    pub stream_type: StreamTypeId,
    /// Sequence within the speaker/channel stream.
    pub channel_seq: u16,
    /// Transport sequence used for loss and reordering diagnostics.
    pub transport_seq: u64,
    /// Codec payload.  Codec metadata comes from the server metadata response.
    pub payload: Bytes,
}

/// The public name used by adapters that consume received media.
pub type MediaFrame = InboundMedia;
